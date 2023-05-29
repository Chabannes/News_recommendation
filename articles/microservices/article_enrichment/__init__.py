import logging
import json
import time
import requests
import asyncio
import os


import azure.functions as func
from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.process.traversal import T
from azure.eventhub.aio import EventHubConsumerClient

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from sentence_transformers import SentenceTransformer, util

from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from datetime import datetime, timedelta


import nest_asyncio
nest_asyncio.apply()
import asyncio




cosmosDB_endpoint = 'wss://cosmosdb-amavla-recommendation.gremlin.cosmos.azure.com:443/'
cosmos_database_name = 'news-recommendation-db'
graph_name = 'graph_articles_users'
cosmosDB_key = 'H4WjsCs5ebeXj8j7K2lwW9ZtBLU4kZabojX2XouEuL6y55UWXwtflPQYOqSX5kUDu7vzctMmGRHrACDbFFs6og=='

eventhub_connection_string = "Endpoint=sb://eventhub-amavla-newsarticles.servicebus.windows.net/;SharedAccessKeyName=listen_articleEventConnectionString;SharedAccessKey=8uOq3a4zKa0qZq1S3cowTiWfKPGc8FL8h+AEhJVPDRU=;EntityPath=event-hub-article-generation"
eventhub_name = "event-hub-article-generation"
consumer_group = "$Default"

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
sim_threshold = 0.3

# checkpointing event hub events
storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=blobamavlareco;AccountKey=k59k85Pia8dw7BLlINoO/VSzX0vC1Afp7jzaPiLXnat0U38m7J5rsfKQiL93Uw3MoAeUcA1P4HXI+AStqbGgmw==;EndpointSuffix=core.windows.net"
container_name = "eventhub-checkpoints"


checkpoint_store = BlobCheckpointStore.from_connection_string(storage_connection_string, container_name)


async def get_best_tags(properties):
    tags = ['sport', 'tech', 'politics', 'entertainment', 'business']
    best_tags = []
    for tag in tags:
        if properties.get(tag) and properties[tag] <= 2:
            best_tags.append(tag)
    return best_tags


async def get_labels(text):


    headers = {
        'Authorization': 'Bearer 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',
        'Content-Type': 'application/json'
    }

    data = '{"inputs": "%s"}' % text[:512].replace('"', '\\"').replace("'", "\\'").encode('utf-8', 'replace')
    url = 'https://api-inference.huggingface.co/models/abhishek/autonlp-bbc-news-classification-37229289'

    logging.info('TEXT %s', text)

    response = requests.post(url, headers=headers, data=data)

    logging.info('**RESPONSE: %s', response.text)

    t = 0
    while True and t < 10:

        response = requests.post(url, headers=headers, data=data)
        
        if response.ok:
            # Extract the list of labels and scores from the predictions
            label_scores = response.json()[0]
            
            # Sort the labels based on the scores
            sorted_labels = sorted(label_scores, key=lambda x: x['score'], reverse=True)
            
            # Create a dictionary with the ranking of each label
            rankings = {}
            for i, label in enumerate(sorted_labels):
                rankings[label['label']] = i + 1

            logging.info('**RANKINGS: %s', rankings)

            return rankings
        
        elif "currently loading" in response.text:
            estimated_time = response.json()["estimated_time"]
            logging.info("Model is currently loading, waiting for %.2f seconds before retrying", 8)
            await asyncio.sleep(8)            
            t = t + 1
        else:
            raise ValueError(f"Failed to get response from Hugging Face API: {response.text}")


    logging.info('**Error in model-tagging API call: %s', response.text)
    raise ValueError('Could not get answer model-tagging API: %s', response.text)



async def process_articles(partition_context, event_batch):
    for event_data in event_batch:
        
        # Process each event
        try:

            message_body = event_data.body_as_str()
            message_dict = json.loads(message_body)
            
            source = message_dict.get('source')
            title = message_dict.get('title')
            publishedAt = message_dict.get('publishedAt')
            content = message_dict.get('content')
            
            new_article = {
                'source': source,
                'title': title,
                'publishedAt': publishedAt,
                'content': content
            }

            await insert_article(new_article)

            await partition_context.update_checkpoint(event_data)  # Checkpoint the processed event


            await asyncio.sleep(5)
            
            
        except Exception as e:
            print(f"Error processing event hub message: {str(e)}")


async def insert_article(new_article):

    # Create a Gremlin client and connect to the Cosmos DB graph
    gremlin_client = client.Client(cosmosDB_endpoint, 'g',
                                        username="/dbs/%s/colls/%s" % (cosmos_database_name, graph_name),
                                        password=cosmosDB_key,
                                        message_serializer=serializer.GraphSONSerializersV2d0()
                                        )

    # Check if article is already in DB
    # Define the Gremlin query to check if there is an existing article with similar title and source
    check_query_already_exist = """
        g.V().has('article', 'title', title).has('source', source)
                                """

    # Execute the Gremlin query with the given article properties to check if an article already exists
    result_set = gremlin_client.submit(check_query_already_exist, {
            'title': new_article['title'],
            'source': new_article['source']
            })

    # If there are no existing articles, add the new article vertex
    if not result_set.all().result():
        # Define the Gremlin query to insert the article vertex

        # Create a dictionary of properties for the new article
        properties = {
                    'title': new_article['title'],
                    'content': new_article['content'],
                    'source': new_article['source'],
                    'publishedAt': new_article['publishedAt'],
                    'partitionKey': new_article['publishedAt']
        }
                

        # Add properties for each tag and its corresponding score
        label_scores = await get_labels(new_article['title'])

        for tag, score in label_scores.items():
            properties[tag] = score


        # get the best tags of the new article
        best_tags = await get_best_tags(properties)

        # Construct the query to select articles with the specified properties and label
        query = "g.V().hasLabel('article')"
        for tag in best_tags:
            query += ".has('{}', lte(2))".format(tag)
        query += ".limit(200).values('title').fold()"

        # Get the same-category articles
        all_titles = gremlin_client.submit(query)
        all_titles = all_titles.all().result()
        all_titles = list(all_titles[0])

            
        # Construct the Gremlin query with the properties
        add_query = """
                    g.addV('article')
                        .property('title', title)
                        .property('content', content)
                        .property('source', source)
                        .property('publishedAt', publishedAt)
                        .property('partitionKey', partitionKey)
                    """

        # Add properties for each tag and its corresponding score to the query
        for tag, score in label_scores.items():
            add_query += f"\n.property('{tag}', {score})"

        # Execute the Gremlin query with the given article properties
        result_set = gremlin_client.submitAsync(add_query, properties)
        print('**INFO: Article successfully inserted')


        # Delete latest article (replacing the oldest article with the newest)
        query = "g.V().hasLabel('article').order().by('publishedAt').limit(1).sideEffect(drop())"
        result_set = gremlin_client.submit(query)


        # Loop over all titles in the list and compute similarity
        for title in all_titles:

            relationship_query = """
                        g.V().has('article', 'title', title1).as('a')
                            .V().has('article', 'title', title2).as('b')
                            .coalesce(
                                select('a').outE('similarity').where(inV().as('b')),
                                addE('similarity').from('a').to('b').property('value', sim)
                            )
                                        """

            # Query the content property of the article with the matching title

            query = "g.V().hasLabel('article').has('title', title).values('content').fold()"
            content = gremlin_client.submit(query, {'title': title}).all().result()
            content = list(content[0])

            embedding_new_article = model.encode(new_article['content'], convert_to_tensor=True)
            embedding_article = model.encode(content, convert_to_tensor=True)
            sim = util.pytorch_cos_sim(embedding_new_article, embedding_article)[0][0].item()

            # If the similarity score is above the threshold, create a relationship between the articles
            if sim >= sim_threshold:
                # Create a relationship between the new article and the current article
                result_set = gremlin_client.submitAsync(relationship_query, {
                                'title1': new_article['title'],
                                'title2': title,
                                'sim': sim
                        })

                print('**INFO: similarity = %s' % round(sim, 2))

    else:
        print('**INFO: Article is already in Database')

    
    # Close the Gremlin client connection
    gremlin_client.close()



async def read_articles():
    consumer_client = EventHubConsumerClient.from_connection_string(
        eventhub_connection_string, eventhub_name=eventhub_name, consumer_group=consumer_group,
        checkpoint_store=checkpoint_store
    )

    try:
        await consumer_client.receive_batch(
            on_event_batch=process_articles,
            starting_position="-1",
            max_wait_time=5,
            max_batch_size=1
        )
    finally:
        await consumer_client.close()



# Run the event receiving function
asyncio.run(read_articles())
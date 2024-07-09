use elasticsearch::{http::transport::Transport, Elasticsearch, Error, SearchParts};
use serde_json::{json, Value};
use futures::future::join_all;
use tokio::spawn;
use std::convert::TryInto;
use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{
        BasicPublishArguments, QueueBindArguments, QueueDeclareArguments,
    },
    connection::{Connection, OpenConnectionArguments},
    consumer::DefaultConsumer,
    BasicProperties,
};
use tokio::time;
use tokio::task;
use tracing_subscriber::{fmt::*,prelude::*, EnvFilter};
use amqprs::error::Error as OtherError;

async fn elastic_search(client: &Elasticsearch, index: &str, from: usize) -> Result<Vec<String>, Error> {
    let from_i64 = from.try_into().unwrap();
    let response = client
        .search(SearchParts::Index(&[index]))
        .from(from_i64)
        .size(10000)
        .scroll("1m")
        .body(json!({
            "query": {
                "match_all": {}
            }
        }))
        .send()
        .await?;
    let empty_vec = Vec::new();
    let response_body: Value = response.json().await?;
        let hits = response_body["hits"]["hits"].as_array().unwrap_or_else(|| &empty_vec);

        let messages: Vec<String> = hits.iter()
            .map(|hit| hit["_source"]["binario"].as_str().unwrap_or("teste").to_string())
            .collect();
    Ok(messages)
}

async fn rabbit_publish(messages: Vec<String>) -> Result<(), OtherError> {
    let connection = Connection::open(&OpenConnectionArguments::new("localhost", 5672, "usuario", "senha"))
        .await?;
    connection.register_callback(DefaultConnectionCallback).await?;

    let channel = connection.open_channel(None).await?;
    channel.register_callback(DefaultChannelCallback).await?;

    let (queue_name, _, _) = channel
        .queue_declare(QueueDeclareArguments::durable_client_named("queue"))
        .await?
        .unwrap();

    channel.queue_bind(QueueBindArguments::new(&queue_name, "topic", "queue")).await?;

    let publish_futures = messages.into_iter().map(|message| {
        let channel = channel.clone();
        let args = BasicPublishArguments::new("topic", "queue");
        let content = message.into_bytes();
        async move {
            channel.basic_publish(BasicProperties::default(), content, args).await
        }
    });

    let _results: Vec<_> = join_all(publish_futures).await;

    channel.close().await?;
    connection.close().await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(layer())
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();

    let transport = Transport::single_node("endereço do elasticsearch")?;
    let client = Elasticsearch::new(transport);

    let index = "indice_elasticsearch";
    let mut tasks = vec![];

    for i in 0..250 {
        let client = client.clone();
        let task = task::spawn(async move {
            match elastic_search(&client, index, i * 100).await {
                Ok(messages) => {
                    rabbit_publish(messages).await.unwrap();
                },
                Err(e) => eprintln!("Erro na pesquisa: {:?}", e),
            }
        });
        tasks.push(task);
    }

    join_all(tasks).await;
    Ok(())
}

use meilisearch_sdk::client::Client;
use reqwest::Client as ReqwestClient;
use reqwest::Certificate;
use serde_json::json;
use serde_json::Value;

async fn fetch_data_from_opensearch(client: &ReqwestClient, index_name: &str, scroll_id: Option<&str>) -> (Vec<Value>, Option<String>) {
    let (url, body);

    if let Some(scroll_id) = scroll_id {
        url = "http://localhost:9200/_search/scroll".to_string();
        body = json!({
            "scroll": "1m",
            "scroll_id": scroll_id
        });
    } else {
        url = format!("http://localhost:9200/{}/_search?scroll=1m", index_name);
        body = json!({
            "size": 1000,
            "query": { "match_all": {} }
        });
    }

    let response = client.post(&url)
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();


    let hits = response["hits"]["hits"].as_array().unwrap_or(&vec![]).iter().map(|doc| doc["_source"].clone()).collect();
    let scroll_id = response["_scroll_id"].as_str().map(|s| s.to_string());

    (hits, scroll_id)
}

async fn index_data_in_meilisearch(data: &[Value], index_name: &str) {
    let meilisearch_client = Client::new("http://localhost:7700", Some("api key")).expect("Failed to create MeiliSearch client");
    let index = meilisearch_client.index(index_name);
    let result = index.add_documents(data, None).await.unwrap();
    println!("MeiliSearch insertion result: {:?}", result);
}

#[tokio::main]
async fn main() {
    let opensearch_client = ReqwestClient::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let index_name = "indice do opensearch";
    let mut scroll_id = None;

    loop {
        let (data, new_scroll_id) = fetch_data_from_opensearch(&opensearch_client, index_name, scroll_id.as_deref()).await;
        if data.is_empty() {
            break;
        }
        println!("Fetched {} documents", data.len());
        index_data_in_meilisearch(&data, "indice do meilisearch).await;
        println!("Indexed {} documents", data.len());
        scroll_id = new_scroll_id;
    }
}

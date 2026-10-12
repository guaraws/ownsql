use tokio_postgres::NoTls;
#[tokio::test]
#[ignore = "requires server; set OWNSQL_TEST_URL"]
async fn prepared_binary_and_portals() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("OWNSQL_TEST_URL")?;
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await?;
    tokio::spawn(async move { connection.await.unwrap() });
    let r = client
        .query_one(
            "SELECT $1::bigint, $2::boolean, $3::text",
            &[&42i64, &true, &Option::<String>::None],
        )
        .await?;
    assert_eq!(r.get::<_, i64>(0), 42);
    assert!(r.get::<_, bool>(1));
    assert_eq!(r.get::<_, Option<String>>(2), None);
    client
        .batch_execute("CREATE TABLE binary_driver (id BIGINT PRIMARY KEY, stock INTEGER)")
        .await?;
    client
        .execute("INSERT INTO binary_driver VALUES ($1,$2)", &[&1i64, &10i32])
        .await?;
    let prepared = client
        .prepare("SELECT stock FROM binary_driver WHERE id=$1")
        .await?;
    assert_eq!(
        client
            .query_one(&prepared, &[&1i64])
            .await?
            .get::<_, i32>(0),
        10
    );
    assert_eq!(
        client
            .query_one(
                "UPDATE binary_driver SET stock=stock-$1 WHERE id=$2 RETURNING stock",
                &[&2i32, &1i64]
            )
            .await?
            .get::<_, i32>(0),
        8
    );
    client.batch_execute("DROP TABLE binary_driver").await?;
    Ok(())
}

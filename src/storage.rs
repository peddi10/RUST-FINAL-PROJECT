//! This module handles the storage of data into a PostgreSQL database.
//!
//! It provides functions for creating a connection pool, storing data from DataFrames, and retrieving data from the database.

use anyhow::{Context, Result};
use futures::future::try_join_all;
use polars::prelude::*;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use bigdecimal::BigDecimal;
use std::str::FromStr;

fn main() {
    // Convert f64 to BigDecimal
    let fixed_acidity_bd = BigDecimal::from_str(&fixed_acidity_f64.to_string())
        .expect("Failed to convert to BigDecimal");

    // Now you can use fixed_acidity_bd as a BigDecimal
    println!("Fixed acidity as BigDecimal: {}", fixed_acidity_bd);
}



/// Creates a connection pool to the PostgreSQL database.
///
/// # Returns
///
/// * `Result<PgPool>` - A result containing the PostgreSQL connection pool if successful, or an error if the connection setup fails.
///
/// # Example
///
/// ```
/// let pool = create_connection_pool().await.expect("Failed to create connection pool");
/// ```
pub async fn create_connection_pool() -> Result<PgPool> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    Ok(pool)
}

/// Stores data from a DataFrame into the PostgreSQL database.
///
/// # Arguments
///
/// * `pool` - A reference to the PostgreSQL connection pool.
/// * `df` - A reference to the DataFrame containing the data to be stored.
///
/// # Returns
///
/// * `Result<()>` - A result indicating success or failure of the data storage operation.
///
/// # Example
///
/// ```
/// let df = DataFrame::new(vec![
///     Series::new("fixed acidity", &vec![7.4, 7.8]),
///     Series::new("volatile acidity", &vec![0.7, 0.88]),
///     // other columns...
/// ]).unwrap();
///
/// store_data(&pool, &df).await.expect("Failed to store data");
/// ```
pub async fn store_data(pool: &PgPool, df: &DataFrame) -> Result<()> {
    let fixed_acidity_series = df.column("fixed acidity")?.f64()?;
    let volatile_acidity_series = df.column("volatile acidity")?.f64()?;
    let citric_acid_series = df.column("citric acid")?.f64()?;
    let residual_sugar_series = df.column("residual sugar")?.f64()?;
    let chlorides_series = df.column("chlorides")?.f64()?;
    let free_sulfur_dioxide_series = df.column("free sulfur dioxide")?.i32()?;
    let total_sulfur_dioxide_series = df.column("total sulfur dioxide")?.i32()?;
    let density_series = df.column("density")?.f64()?;
    let ph_series = df.column("pH")?.f64()?;
    let sulphates_series = df.column("sulphates")?.f64()?;
    let alcohol_series = df.column("alcohol")?.f64()?;
    let quality_series = df.column("quality")?.i32()?;

    let mut tasks = vec![];

    for i in 0..df.height() {
        let fixed_acidity = fixed_acidity_series.get(i).context("Failed to get fixed acidity")?;
        let volatile_acidity = volatile_acidity_series.get(i).context("Failed to get volatile acidity")?;
        let citric_acid = citric_acid_series.get(i).context("Failed to get citric acid")?;
        let residual_sugar = residual_sugar_series.get(i).context("Failed to get residual sugar")?;
        let chlorides = chlorides_series.get(i).context("Failed to get chlorides")?;
        let free_sulfur_dioxide = free_sulfur_dioxide_series.get(i).context("Failed to get free sulfur dioxide")? as f64;
        let total_sulfur_dioxide = total_sulfur_dioxide_series.get(i).context("Failed to get total sulfur dioxide")? as f64;
        let density = density_series.get(i).context("Failed to get density")?;
        let ph = ph_series.get(i).context("Failed to get pH")?;
        let sulphates = sulphates_series.get(i).context("Failed to get sulphates")?;
        let alcohol = alcohol_series.get(i).context("Failed to get alcohol")?;
        let quality = quality_series.get(i).context("Failed to get quality")?;

        let pool = pool.clone();
        let task = tokio::spawn(async move {
            let result = sqlx::query!(
                r#"
                INSERT INTO wine_quality (fixed_acidity, volatile_acidity, citric_acid, residual_sugar, chlorides, free_sulfur_dioxide, total_sulfur_dioxide, density, pH, sulphates, alcohol, quality)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                "#,
                fixed_acidity,
                volatile_acidity,
                citric_acid,
                residual_sugar,
                chlorides,
                free_sulfur_dioxide,
                total_sulfur_dioxide,
                density,
                ph,
                sulphates,
                alcohol,
                quality
            )
            .execute(&pool)
            .await;

            if let Err(e) = &result {
                eprintln!("Failed to insert row {}: {:?}", i, e);
            }

            result.context("Failed to insert data into the database")
        });

        tasks.push(task);
    }

    try_join_all(tasks).await?;
    Ok(())
}

/// Fetches and prints the first 5 rows from the wine_quality table in the PostgreSQL database.
///
/// # Arguments
///
/// * `pool` - A reference to the PostgreSQL connection pool.
///
/// # Returns
///
/// * `Result<()>` - A result indicating success or failure of the data retrieval operation.
///
/// # Example
///
/// ```
/// get_first_5_rows(&pool).await.expect("Failed to fetch first 5 rows");
/// ```
pub async fn get_first_5_rows(pool: &PgPool) -> Result<()> {
    let rows = sqlx::query("SELECT * FROM wine_quality LIMIT 5")
        .fetch_all(pool)
        .await
        .context("Failed to fetch rows from the database")?;

    for row in rows {
        let id: i32 = row.try_get("id")?;
        let fixed_acidity: f64 = row.try_get("fixed_acidity")?;
        let volatile_acidity: f64 = row.try_get("volatile_acidity")?;
        let citric_acid: f64 = row.try_get("citric_acid")?;
        let residual_sugar: f64 = row.try_get("residual_sugar")?;
        let chlorides: f64 = row.try_get("chlorides")?;
        let free_sulfur_dioxide: f64 = row.try_get("free_sulfur_dioxide")?;
        let total_sulfur_dioxide: f64 = row.try_get("total_sulfur_dioxide")?;
        let density: f64 = row.try_get("density")?;
        let ph: f64 = row.try_get("pH")?;
        let sulphates: f64 = row.try_get("sulphates")?;
        let alcohol: f64 = row.try_get("alcohol")?;
        let quality: i32 = row.try_get("quality")?;

        println!(
            "ID: {}, Fixed Acidity: {}, Volatile Acidity: {}, Citric Acid: {}, Residual Sugar: {}, Chlorides: {}, Free Sulfur Dioxide: {}, Total Sulfur Dioxide: {}, Density: {}, pH: {}, Sulphates: {}, Alcohol: {}, Quality: {}",
            id, fixed_acidity, volatile_acidity, citric_acid, residual_sugar, chlorides, free_sulfur_dioxide, total_sulfur_dioxide, density, ph, sulphates, alcohol, quality
        );
    }

    Ok(())
}


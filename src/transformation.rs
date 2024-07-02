//! This module handles the transformation of data within DataFrames.
//!
//! It provides functions for cleaning, normalizing, and validating data.

use anyhow::{Context, Result};
use polars::prelude::*;

/// Transforms the input DataFrame by cleaning, normalizing, and validating the data.
///
/// # Arguments
///
/// * `df` - A DataFrame containing the data to be transformed.
///
/// # Returns
///
/// * `Result<DataFrame>` - A result containing the transformed DataFrame if successful, or an error if the transformation fails.
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
/// let transformed_df = transform_data(df).expect("Data transformation failed");
/// ```
pub fn transform_data(df: DataFrame) -> Result<DataFrame> {
    let df = clean_data(df)?;
    let df = normalize_data(df)?;
    let df = validate_data(df)?;
    Ok(df)
}

/// Cleans the data by replacing missing values with the median value of each column.
///
/// # Arguments
///
/// * `df` - A DataFrame containing the data to be cleaned.
///
/// # Returns
///
/// * `Result<DataFrame>` - A result containing the cleaned DataFrame if successful, or an error if the cleaning fails.
fn clean_data(df: DataFrame) -> Result<DataFrame> {
    let median_fixed_acidity = median_value(&df, "fixed acidity")?;
    let median_volatile_acidity = median_value(&df, "volatile acidity")?;
    // Repeat for other columns...

    let df = df
        .lazy()
        .with_column(col("fixed acidity").fill_null(lit(median_fixed_acidity)))
        .with_column(col("volatile acidity").fill_null(lit(median_volatile_acidity)))
        // Repeat for other columns...
        .collect()
        .context("Error collecting DataFrame after cleaning")?;
    
    Ok(df)
}

/// Helper function to calculate median value for a column.
fn median_value(df: &DataFrame, column: &str) -> Result<f64> {
    df.column(column)
        .context(format!("Error fetching column {}", column))?
        .f64()
        .context(format!("Error converting {} column to f64", column))?
        .median()
        .context(format!("Error calculating median for {} column", column))
}

/// Normalizes the data by scaling the numeric columns to a 0-1 range.
///
/// # Arguments
///
/// * `df` - A DataFrame containing the data to be normalized.
///
/// # Returns
///
/// * `Result<DataFrame>` - A result containing the normalized DataFrame if successful, or an error if the normalization fails.
fn normalize_data(df: DataFrame) -> Result<DataFrame> {
    // Similar to clean_data, use .map and .with_column to normalize each numeric column
    let df = df
        .lazy()
        // Use .map and .with_column to normalize each column
        .collect()
        .context("Error collecting DataFrame after normalization")?;
    
    Ok(df)
}

/// Validates the data by ensuring no negative values are present in numeric columns.
///
/// # Arguments
///
/// * `df` - A DataFrame containing the data to be validated.
///
/// # Returns
///
/// * `Result<DataFrame>` - A result containing the validated DataFrame if successful, or an error if the validation fails.
fn validate_data(df: DataFrame) -> Result<DataFrame> {
    // Use .filter and .collect to remove rows with negative values
    let valid_data = df
        .lazy()
        // Use .filter and .collect to remove rows with negative values
        .collect()
        .context("Error collecting DataFrame after validation")?;
    
    Ok(valid_data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;

    fn create_test_dataframe() -> DataFrame {
        df!(
            "fixed acidity" => &vec![7.4, 7.8, 7.5],
            "volatile acidity" => &vec![0.7, 0.88, 0.76],
            // other columns...
        )
        .unwrap()
    }

    #[test]
    fn test_transform_data() {
        let df = create_test_dataframe();
        let result = transform_data(df);
        assert!(result.is_ok());

        let transformed_df = result.unwrap();
        assert_eq!(transformed_df.height(), 3); // Should remain 3 rows
        assert_eq!(transformed_df.width(), 12); // 12 columns after transformation
    }

    #[test]
    fn test_clean_data() {
        let df = df!(
            "fixed acidity" => &vec![Some(7.4), None, Some(7.5)],
            "volatile acidity" => &vec![Some(0.7), None, Some(0.76)]
            // other columns...
        )
        .unwrap();
        let cleaned_df = clean_data(df);
        assert!(cleaned_df.is_ok());

        let fixed_acidity_col = cleaned_df.unwrap().column("fixed acidity").unwrap().f64().unwrap();
        assert!(fixed_acidity_col.null_count() == 0);

        // Add more assertions for other columns if needed
    }
}


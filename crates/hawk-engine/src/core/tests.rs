use crate::core::{
    canonical_dimension_key, dimension_key_from_pairs, parse_canonical_dimension_key,
    DimensionDefinition, DistributionRepr, HawkError, Schema, VariableDefinition, VariableType,
    UNKNOWN_CATEGORY_LABEL,
};
use crate::math::entropy;

#[test]
fn schema_variable_uniqueness() {
    let mut schema = Schema::default();
    schema
        .define_variable(VariableDefinition {
            name: "sentiment".to_string(),
            var_type: VariableType::Continuous {
                bins: 50,
                range: None,
            },
        })
        .expect("first define should pass");

    assert!(schema
        .define_variable(VariableDefinition {
            name: "sentiment".to_string(),
            var_type: VariableType::Continuous {
                bins: 10,
                range: None,
            },
        })
        .is_err());
}

#[test]
fn canonical_dimension_is_stable() {
    let key = dimension_key_from_pairs([("time", "2024-03"), ("topic", "russia-ukraine")]);
    assert_eq!(
        canonical_dimension_key(&key),
        "time:2024-03/topic:russia-ukraine"
    );
}

#[test]
fn canonical_dimension_key_escapes_separators() {
    let key = dimension_key_from_pairs([("time:period", "2024/03"), ("topic/name", "a:b/c%done")]);

    let canonical = canonical_dimension_key(&key);

    assert_eq!(
        canonical,
        "time%3Aperiod:2024%2F03/topic%2Fname:a%3Ab%2Fc%25done"
    );
    assert_eq!(parse_canonical_dimension_key(&canonical), Some(key));
}

#[test]
fn canonical_dimension_key_rejects_invalid_escape_sequences() {
    assert!(parse_canonical_dimension_key("topic:a%2").is_none());
    assert!(parse_canonical_dimension_key("topic:a%XX").is_none());
    assert!(parse_canonical_dimension_key("missing-separator").is_none());
}

#[test]
fn repr_probability_vector_handles_zero_count() {
    let repr = DistributionRepr::Histogram {
        min: 0.0,
        max: 1.0,
        bin_counts: vec![0, 0, 0],
        total_count: 0,
    };
    assert_eq!(repr.as_probability_vector(), vec![0.0, 0.0, 0.0]);
}

#[test]
fn schema_rejects_invalid_continuous_ranges() {
    let mut schema = Schema::default();

    assert!(schema
        .define_variable(VariableDefinition {
            name: "sentiment".to_owned(),
            var_type: VariableType::Continuous {
                bins: 10,
                range: Some((1.0, 1.0)),
            },
        })
        .is_err());

    assert!(schema
        .define_variable(VariableDefinition {
            name: "score".to_owned(),
            var_type: VariableType::Continuous {
                bins: 10,
                range: Some((f64::NAN, 1.0)),
            },
        })
        .is_err());
}

#[test]
fn schema_rejects_duplicate_and_empty_categories() {
    let mut schema = Schema::default();

    assert!(schema
        .define_variable(VariableDefinition {
            name: "category".to_owned(),
            var_type: VariableType::Categorical {
                categories: vec!["a".to_owned(), "a".to_owned()],
                allow_unknown: false,
            },
        })
        .is_err());

    assert!(schema
        .define_variable(VariableDefinition {
            name: "other".to_owned(),
            var_type: VariableType::Categorical {
                categories: vec!["".to_owned()],
                allow_unknown: false,
            },
        })
        .is_err());
}

#[test]
fn schema_rejects_invalid_dimension_definitions() {
    let mut schema = Schema::default();

    assert!(schema
        .define_dimension(DimensionDefinition {
            name: "".to_owned(),
            source_column: "time".to_owned(),
            granularity: None,
        })
        .is_err());

    assert!(schema
        .define_dimension(DimensionDefinition {
            name: "time".to_owned(),
            source_column: " ".to_owned(),
            granularity: None,
        })
        .is_err());
}

#[test]
fn schema_rejects_self_joint_definition() {
    let mut schema = Schema::default();
    schema
        .define_variable(VariableDefinition {
            name: "category".to_owned(),
            var_type: VariableType::Categorical {
                categories: vec!["a".to_owned(), "b".to_owned()],
                allow_unknown: false,
            },
        })
        .expect("valid variable");

    assert!(schema.define_joint("category", "category").is_err());
}

#[test]
fn empty_categorical_distribution_has_zero_known_and_unknown_mass() {
    let repr = categorical_repr();

    assert_eq!(repr.total_count(), 0);
    assert_eq!(repr.value_count_vector(), vec![0, 0, 0]);
    assert_eq!(repr.as_probability_vector(), vec![0.0, 0.0, 0.0]);
    assert_eq!(
        repr.categorical_labels_with_unknown().unwrap(),
        vec!["red", "blue", UNKNOWN_CATEGORY_LABEL]
    );
}

#[test]
fn known_only_categorical_distribution_counts_and_probabilities_sum_to_total() {
    let mut repr = categorical_repr();

    repr.increment_categorical(Some(0), 2).unwrap();
    repr.increment_categorical(Some(1), 1).unwrap();

    let counts = repr.value_count_vector();
    let probs = repr.as_probability_vector();
    assert_eq!(repr.total_count(), 3);
    assert_eq!(counts, vec![2, 1, 0]);
    assert_eq!(counts.iter().sum::<u64>(), repr.total_count());
    assert!((probs.iter().sum::<f64>() - 1.0).abs() < 1e-12);
}

#[test]
fn unknown_only_categorical_distribution_exposes_unknown_bucket() {
    let mut repr = categorical_repr();

    repr.increment_categorical(None, 4).unwrap();

    let counts = repr.value_count_vector();
    let probs = repr.as_probability_vector();
    assert_eq!(repr.total_count(), 4);
    assert_eq!(counts, vec![0, 0, 4]);
    assert_eq!(
        repr.categorical_labels_with_unknown().unwrap(),
        vec!["red", "blue", UNKNOWN_CATEGORY_LABEL]
    );
    assert_eq!(probs, vec![0.0, 0.0, 1.0]);
    assert_eq!(counts.iter().sum::<u64>(), repr.total_count());
}

#[test]
fn mixed_categorical_distribution_includes_unknown_probability_mass() {
    let mut repr = categorical_repr();

    repr.increment_categorical(Some(0), 2).unwrap();
    repr.increment_categorical(None, 1).unwrap();
    repr.increment_categorical(Some(1), 1).unwrap();

    let counts = repr.value_count_vector();
    let probs = repr.as_probability_vector();
    assert_eq!(counts, vec![2, 1, 1]);
    assert_eq!(counts.iter().sum::<u64>(), repr.total_count());
    assert!((probs.iter().sum::<f64>() - 1.0).abs() < 1e-12);
    assert_eq!(probs, vec![0.5, 0.25, 0.25]);
}

#[test]
fn invalid_categorical_index_errors_without_partial_mutation() {
    let mut repr = categorical_repr();
    repr.increment_categorical(Some(0), 2).unwrap();

    let before = repr.clone();
    let err = repr.increment_categorical(Some(9), 3).unwrap_err();

    assert!(matches!(err, HawkError::SchemaValidation(_)));
    assert_eq!(repr, before);
}

#[test]
fn categorical_entropy_accounts_for_unknown_bucket() {
    let mut known_only = categorical_repr();
    known_only.increment_categorical(Some(0), 2).unwrap();
    known_only.increment_categorical(Some(1), 2).unwrap();

    let mut with_unknown = known_only.clone();
    with_unknown.increment_categorical(None, 2).unwrap();

    let known_entropy = entropy(&known_only.value_count_vector(), known_only.total_count());
    let unknown_entropy = entropy(
        &with_unknown.value_count_vector(),
        with_unknown.total_count(),
    );

    assert!((known_entropy - 1.0).abs() < 1e-12);
    assert!((unknown_entropy - 1.584962500721156).abs() < 1e-12);
    assert!(unknown_entropy > known_entropy);
}

fn categorical_repr() -> DistributionRepr {
    DistributionRepr::Categorical {
        categories: vec!["red".to_owned(), "blue".to_owned()],
        counts: vec![0, 0],
        unknown_count: 0,
        total_count: 0,
    }
}

#[cfg(test)]
mod tests {
    use crate::{dimension_key_from_pairs, DistributionRepr, Schema, VariableDefinition, VariableType};

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
        let key = dimension_key_from_pairs([
            ("time", "2024-03"),
            ("topic", "russia-ukraine"),
        ]);
        assert_eq!(crate::canonical_dimension_key(&key), "time:2024-03/topic:russia-ukraine");
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
}

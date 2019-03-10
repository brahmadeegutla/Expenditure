app_config = {
        "source": {
            "bofa_checking": "local_test_data/source/bofa_checking_csv/",
            "bofa_credit": "local_test_data/source/bofa_credit_pdf/",
            "discover": "local_test_data/source/discover_csv/",
            "citi": "local_test_data/source/citi_csv/",
            "chase": "local_test_data/source/chase_csv/"
        },
        "target": {
            "bofa_cc_master": "local_test_data/target/bofa_cc_master/",
            "bofa_chk_master": "local_test_data/target/bofa_chk_master/",
            "discover_master": "local_test_data/target/discover_master/",
            "citi_master": "local_test_data/target/citi_master/",
            "chase_master": "local_test_data/target/chase_master/",
            "all_master": "local_test_data/target/all_master",
            "new_descriptions": "local_test_data/lookup/new_descriptions.csv"
        },
        "lookup": {
            "category_flags": "local_test_data/lookup/category_flags.csv",
            "description_flags": "local_test_data/lookup/description_flags.csv"
        }
    }


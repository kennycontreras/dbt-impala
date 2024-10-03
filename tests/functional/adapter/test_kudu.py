import pytest
from dbt.tests.util import run_dbt, relation_from_name, check_relations_equal

from dbt.tests.adapter.basic.test_incremental import BaseIncremental

from dbt.tests.adapter.basic.files import (
    schema_base_yml,
    model_incremental,
)

basic_model_sql = """
{{
       config(
           stored_as = "kudu",
           materialized = "table",
           primary_key='(id)'
      )
    }}
   select * from {{ ref('kudu_ref_table')}}
""".lstrip()

seed_csv = """
id, name, country
1, Niranjan, India
2, Nitesh, USA
3, Kasa, Hungary
4, Archit, Pune
5, Gopi, India
""".lstrip()


class TestBasic:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"kudu_ref_table.csv": seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"basic_model.sql": basic_model_sql}

    @pytest.mark.kudu
    def test_basic(self, project):
        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt(["run"])
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "basic_model")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 5


incremental_kudu_sql = (
    """
 {{
    config(
        materialized="incremental",
        stored_as="kudu",
        primary_key='(id)'
    )
}}
""".strip()
    + model_incremental
)


class TestIncrementalKudu(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental_test_model"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_test_model.sql": incremental_kudu_sql, "schema.yml": schema_base_yml}

    @pytest.mark.kudu
    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental_test_model"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["added", "incremental_test_model"])

        # run full-refresh and compare with base table again
        results = run_dbt(
            [
                "run",
                "--select",
                "incremental_test_model",
                "--full-refresh",
                "--vars",
                "seed_name: base",
            ]
        )
        assert len(results) == 1

        check_relations_equal(project.adapter, ["base", "incremental_test_model"])

        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1

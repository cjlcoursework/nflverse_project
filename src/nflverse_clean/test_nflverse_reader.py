# Ideas borrowed from @cooperdff
# Original code: https://github.com/cooperdff/nfl_data_py.git
# Modified by: Chris Lomeli
#
import os.path
import unittest
import nflverse_reader as up


class TestNFLVerse(unittest.TestCase):
    file_type = "csv"
    output_directory = "../../output"
    schema_directory = "../../schemas"

    @classmethod
    def setUpClass(cls):
        if not os.path.exists(cls.output_directory):
            os.makedirs(cls.output_directory)

    @classmethod
    def tearDownClass(self):
        pass

    def test_load_pbp(self):
        year = 2021
        file_type = self.file_type
        local_file_path=f"{self.output_directory}/playbyplay_{year}.{file_type}"
        local_schema_path=f"{self.output_directory}/schema_pbp.csv"
        result = up.load_pdp(year, file_type, local_file_path, local_schema_path)
        assert(result == 200)
        assert(os.path.exists(local_file_path))

    def test_load_pbp_participation(self):
        year = 2021
        file_type = self.file_type
        local_file_path=f"{self.output_directory}/playbyplay{year}_participation.{file_type}"
        result = up.load_pbp_participation(year, file_type, local_file_path, schema_file=f"{self.schema_directory}/schema_pbp_participation.csv")
        assert(result == 200)
        assert(os.path.exists(local_file_path))

    def test_load_injuries(self):
        year= 2021
        file_type = self.file_type
        local_file_path=f"{self.output_directory}/injuries_{year}.{file_type}"
        result = up.load_injuries(year, file_type, local_file_path, schema_file=f"{self.schema_directory}/schema_injuries.csv")
        assert(result == 200)
        assert(os.path.exists(local_file_path))

    def test_load_player_stats(self):
        file_type = self.file_type
    
        # all
        local_file_path=f"{self.output_directory}/player_stats.{file_type}"
        result = up.load_player_stats(
            file_type,
            local_file_path,
            schema_file=f"{self.output_directory}/schema_player_stats.csv")
        assert(result == 200)
        assert(os.path.exists(local_file_path))

        # specific year
        year = 2021
        local_file_path=f"{self.output_directory}/player_stats_{year}.{file_type}"
        result = up.load_player_stats(file_type, local_file_path, year=year )
        assert(result == 200)
        assert(os.path.exists(local_file_path))

    def test_load_pfr_advstats_stats(self):
        file_type = self.file_type
        for stat_type in ['def', 'pass', 'rec', 'rush']:
            year = 2021
            local_file_path=f"{self.output_directory}/pfr_advstats_{stat_type}_{year}.{file_type}"
            result = up.load_pfr_advstats_stats(
                file_type,
                local_file_path,
                stats_type=stat_type,
                year=year,
                schema_file=f"{self.output_directory}/schema_pfr_advstats.csv")
            assert(result == 200)
            assert(os.path.exists(local_file_path))

            local_file_path=f"{self.output_directory}/pfr_advstats_all.{file_type}"
            result = up.load_pfr_advstats_stats(file_type, local_file_path, stats_type=stat_type, schema_file=f"{self.schema_directory}/schema_pfr_advstats.csv")
            assert(result == 200)
            assert(os.path.exists(local_file_path))

    def test_load_players(self):
        file_type = self.file_type
        local_file_path=f"{self.output_directory}/players.{file_type}"
        result = up.load_players(file_type, local_file_path, schema_file=f"{self.schema_directory}/schema_players.csv")
        assert(result == 200)
        assert(os.path.exists(local_file_path))

    def test_load_rosters(self):
        file_type = self.file_type
        year = 2016
        local_file_path=f"{self.output_directory}/rosters.{file_type}"
        result = up.load_rosters(year, file_type, local_file_path, schema_file=f"{self.schema_directory}/schema_rosters.csv")
        assert(result == 200)
        assert(os.path.exists(local_file_path))

    def test_load_rosters_weekly(self):
        file_type = self.file_type
        year = 2016
        local_file_path=f"{self.output_directory}/rosters_week_{year}.{file_type}"
        result = up.load_rosters_weekly(year, file_type, local_file_path, schema_file=f"{self.schema_directory}/schema_rosters_week.csv")
        assert(result == 200)
        assert(os.path.exists(local_file_path))

    def test_load_depth_charts(self):
        file_type = self.file_type
        year = 2016
        local_file_path=f"{self.output_directory}/depth_charts_{year}.{file_type}"
        result = up.load_depth_charts(year, file_type, local_file_path, schema_file=f"{self.schema_directory}/schema_pbp.csv", silent=False)
        assert(result == 200)
        assert(os.path.exists(local_file_path))

    def test_load_next_gen_stats(self):
        file_type = self.file_type + ".gz"
        year = 2016
    
        for stat_type in ['passing', 'rushing', 'receiving']:
            local_file_path=f"{self.output_directory}/next_gen_stats_{stat_type}_{year}.{file_type}"
            result = up.load_next_gen_stats(year, file_type, local_file_path, stat_type=stat_type)
            assert(result == 200)
            assert(os.path.exists(local_file_path))

    def test_load_officials(self):
        file_type = self.file_type
        local_file_path=f"{self.output_directory}/officials.{file_type}"
        result = up.load_officials( file_type, local_file_path)
        assert(result == 200)
        assert(os.path.exists(local_file_path))

    def test_load_snap_counts(self):
        file_type = self.file_type
        year = 2016
        local_file_path=f"{self.output_directory}/snap_counts_{year}.{file_type}"
        result = up.load_snap_counts(year, file_type, local_file_path, schema_file=f"{self.schema_directory}/schema_snap_counts.csv")
        assert(result == 200)
        assert(os.path.exists(local_file_path))


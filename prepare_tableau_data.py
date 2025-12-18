"""
Prepare NBA Data for Tableau Dashboard - Updated Version
This script creates clean, optimized datasets using the comprehensive merged stats
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from config import DB_CONFIG

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def connect_to_database(db_config):
    """Connect to MySQL database"""
    try:
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        if connection.is_connected():
            logging.info("Connected to MySQL database")
            return connection
    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return None

def create_main_dashboard_data(connection):
    """Create the primary comprehensive dataset for Tableau dashboard"""
    
    query = """
    SELECT 
        -- Player Identifiers
        pms.player_name,
        pms.season,
        pms.team,
        pms.position,
        pms.age,
        pms.games_played,
        pms.games_started,
        pms.games_started_percentage,
        pms.awards,
        
        -- Salary Information
        ps.salary,
        ps.salary_formatted,
        ps.salary_rank,
        
        -- Per Game Stats
        pms.pg_minutes_played as mpg,
        pms.pg_points as ppg,
        pms.pg_rebounds as rpg,
        pms.pg_assists as apg,
        pms.pg_steals as spg,
        pms.pg_blocks as bpg,
        pms.pg_turnovers as tov_pg,
        pms.pg_personal_fouls as pf_pg,
        
        -- Per Game Shooting
        pms.pg_field_goal_pct as fg_pct,
        pms.pg_three_point_pct as three_pt_pct,
        pms.pg_free_throw_pct as ft_pct,
        pms.pg_effective_fg_pct as efg_pct,
        pms.pg_three_pointers as three_pm_pg,
        pms.pg_three_point_attempts as three_pa_pg,
        
        -- Total Stats
        pms.total_points,
        pms.total_rebounds,
        pms.total_assists,
        pms.total_steals,
        pms.total_blocks,
        
        -- Advanced Metrics
        pms.adv_player_efficiency_rating as PER,
        pms.adv_true_shooting_pct as TS_pct,
        pms.adv_usage_pct as usage_pct,
        pms.adv_offensive_rebound_pct as ORB_pct,
        pms.adv_defensive_rebound_pct as DRB_pct,
        pms.adv_total_rebound_pct as TRB_pct,
        pms.adv_assist_pct as AST_pct,
        pms.adv_steal_pct as STL_pct,
        pms.adv_block_pct as BLK_pct,
        pms.adv_turnover_pct as TOV_pct,
        
        -- Win Shares & Impact
        pms.adv_offensive_win_shares as OWS,
        pms.adv_defensive_win_shares as DWS,
        pms.adv_win_shares as WS,
        pms.adv_win_shares_per_48 as WS_per_48,
        pms.adv_offensive_box_plus_minus as OBPM,
        pms.adv_defensive_box_plus_minus as DBPM,
        pms.adv_box_plus_minus as BPM,
        pms.adv_value_over_replacement as VORP,
        
        -- Value Metrics
        CASE 
            WHEN pms.pg_points > 0 AND ps.salary > 0 THEN 
                ROUND(pms.pg_points / (ps.salary / 1000000), 2)
            ELSE NULL 
        END AS points_per_million,
        
        CASE 
            WHEN pms.adv_win_shares > 0 AND ps.salary > 0 THEN 
                ROUND(ps.salary / pms.adv_win_shares, 0)
            ELSE NULL 
        END AS salary_per_win_share,
        
        CASE
            WHEN pms.adv_value_over_replacement > 0 AND ps.salary > 0 THEN
                ROUND(ps.salary / pms.adv_value_over_replacement, 0)
            ELSE NULL
        END AS salary_per_vorp,
        
        -- Tier Classifications
        CASE 
            WHEN ps.salary_rank <= 10 THEN 'Top 10 (Supermax)'
            WHEN ps.salary_rank <= 30 THEN 'Top 30 (Max)'
            WHEN ps.salary_rank <= 50 THEN 'Top 50'
            WHEN ps.salary_rank <= 100 THEN 'Top 100'
            WHEN ps.salary_rank <= 200 THEN 'Top 200'
            ELSE 'Other'
        END as salary_tier,
        
        CASE 
            WHEN pms.pg_points >= 25 THEN 'Elite Scorer (25+ PPG)'
            WHEN pms.pg_points >= 20 THEN 'All-Star (20-25 PPG)'
            WHEN pms.pg_points >= 15 THEN 'Starter (15-20 PPG)'
            WHEN pms.pg_points >= 10 THEN 'Role Player (10-15 PPG)'
            ELSE 'Bench Player (<10 PPG)'
        END as scoring_tier,
        
        CASE 
            WHEN pms.adv_value_over_replacement >= 6.0 THEN 'Superstar (6+ VORP)'
            WHEN pms.adv_value_over_replacement >= 4.0 THEN 'Star (4-6 VORP)'
            WHEN pms.adv_value_over_replacement >= 2.0 THEN 'Above Average (2-4 VORP)'
            WHEN pms.adv_value_over_replacement >= 0 THEN 'Average (0-2 VORP)'
            ELSE 'Below Average (<0 VORP)'
        END as impact_tier,
        
        -- Award Flags
        CASE WHEN pms.awards LIKE '%MVP%' THEN 1 ELSE 0 END as is_mvp_candidate,
        CASE WHEN pms.awards LIKE '%AS%' THEN 1 ELSE 0 END as is_allstar,
        CASE WHEN pms.awards LIKE '%NBA1%' THEN 1 ELSE 0 END as is_all_nba_first,
        CASE WHEN pms.awards LIKE '%DPOY%' THEN 1 ELSE 0 END as is_dpoy_candidate,
        
        -- Season Year (for filters)
        CAST(SUBSTRING(pms.season, 1, 4) AS UNSIGNED) as season_year
        
    FROM player_merged_stats pms
    LEFT JOIN player_salaries ps 
        ON pms.player_name = ps.player_name 
        AND pms.season = ps.season
    WHERE pms.games_played >= 10
    ORDER BY pms.season DESC, ps.salary DESC;
    """
    
    logging.info("Fetching main dashboard data...")
    df = pd.read_sql(query, connection)
    logging.info(f"Fetched {len(df)} player-season records")
    
    return df

def create_team_summary_data(connection):
    """Create team-level summary data with advanced metrics"""
    
    query = """
    SELECT 
        pms.season,
        pms.team,
        COUNT(DISTINCT pms.player_name) as roster_count,
        
        -- Salary Information
        SUM(ps.salary) as total_payroll,
        ROUND(AVG(ps.salary), 0) as avg_salary,
        MAX(ps.salary) as max_salary,
        MIN(ps.salary) as min_salary,
        
        -- Basic Stats
        ROUND(AVG(pms.pg_points), 2) as avg_ppg,
        ROUND(AVG(pms.pg_rebounds), 2) as avg_rpg,
        ROUND(AVG(pms.pg_assists), 2) as avg_apg,
        ROUND(AVG(pms.pg_steals), 2) as avg_spg,
        ROUND(AVG(pms.pg_blocks), 2) as avg_bpg,
        
        -- Shooting Efficiency
        ROUND(AVG(pms.pg_field_goal_pct), 3) as avg_fg_pct,
        ROUND(AVG(pms.pg_three_point_pct), 3) as avg_3pt_pct,
        ROUND(AVG(pms.adv_true_shooting_pct), 3) as avg_ts_pct,
        ROUND(AVG(pms.pg_effective_fg_pct), 3) as avg_efg_pct,
        
        -- Advanced Metrics
        ROUND(AVG(pms.adv_player_efficiency_rating), 2) as avg_PER,
        ROUND(SUM(pms.adv_win_shares), 2) as total_WS,
        ROUND(AVG(pms.adv_win_shares), 2) as avg_WS,
        ROUND(SUM(pms.adv_value_over_replacement), 2) as total_VORP,
        ROUND(AVG(pms.adv_value_over_replacement), 2) as avg_VORP,
        ROUND(AVG(pms.adv_box_plus_minus), 2) as avg_BPM,
        ROUND(AVG(pms.adv_offensive_box_plus_minus), 2) as avg_OBPM,
        ROUND(AVG(pms.adv_defensive_box_plus_minus), 2) as avg_DBPM,
        
        -- Total Production
        SUM(pms.total_points) as team_total_points,
        SUM(pms.total_rebounds) as team_total_rebounds,
        SUM(pms.total_assists) as team_total_assists,
        
        -- Value Metrics
        CASE 
            WHEN SUM(pms.adv_win_shares) > 0 THEN 
                ROUND(SUM(ps.salary) / SUM(pms.adv_win_shares), 0)
            ELSE NULL 
        END AS payroll_per_WS,
        
        CASE 
            WHEN SUM(pms.adv_value_over_replacement) > 0 THEN 
                ROUND(SUM(ps.salary) / SUM(pms.adv_value_over_replacement), 0)
            ELSE NULL 
        END AS payroll_per_VORP,
        
        -- Season Year
        CAST(SUBSTRING(pms.season, 1, 4) AS UNSIGNED) as season_year
        
    FROM player_merged_stats pms
    LEFT JOIN player_salaries ps 
        ON pms.player_name = ps.player_name 
        AND pms.season = ps.season
    WHERE pms.games_played >= 10
    GROUP BY pms.season, pms.team
    ORDER BY pms.season DESC, total_VORP DESC;
    """
    
    logging.info("Fetching team summary data...")
    df = pd.read_sql(query, connection)
    logging.info(f"Fetched {len(df)} team-season records")
    
    return df

def create_player_comparison_data(connection):
    """Create year-over-year player comparison data"""
    
    query = """
    SELECT 
        curr.player_name,
        curr.season as current_season,
        curr.team as current_team,
        curr.age as current_age,
        
        -- Current Season Salary
        curr_sal.salary as current_salary,
        curr_sal.salary_formatted as current_salary_formatted,
        curr_sal.salary_rank as current_salary_rank,
        
        -- Current Season Stats
        curr.games_played as current_games,
        curr.pg_points as current_ppg,
        curr.pg_rebounds as current_rpg,
        curr.pg_assists as current_apg,
        curr.adv_player_efficiency_rating as current_PER,
        curr.adv_true_shooting_pct as current_TS_pct,
        curr.adv_win_shares as current_WS,
        curr.adv_value_over_replacement as current_VORP,
        curr.adv_box_plus_minus as current_BPM,
        
        -- Previous Season Info
        prev.season as previous_season,
        prev.team as previous_team,
        prev_sal.salary as previous_salary,
        prev_sal.salary_rank as previous_salary_rank,
        
        -- Previous Season Stats
        prev.games_played as previous_games,
        prev.pg_points as previous_ppg,
        prev.pg_rebounds as previous_rpg,
        prev.pg_assists as previous_apg,
        prev.adv_player_efficiency_rating as previous_PER,
        prev.adv_true_shooting_pct as previous_TS_pct,
        prev.adv_win_shares as previous_WS,
        prev.adv_value_over_replacement as previous_VORP,
        prev.adv_box_plus_minus as previous_BPM,
        
        -- Changes
        (curr_sal.salary - IFNULL(prev_sal.salary, 0)) as salary_change,
        ROUND(((curr_sal.salary - IFNULL(prev_sal.salary, 1)) / IFNULL(prev_sal.salary, 1) * 100), 1) as salary_change_pct,
        ROUND((curr.pg_points - IFNULL(prev.pg_points, 0)), 2) as ppg_change,
        ROUND((curr.pg_rebounds - IFNULL(prev.pg_rebounds, 0)), 2) as rpg_change,
        ROUND((curr.pg_assists - IFNULL(prev.pg_assists, 0)), 2) as apg_change,
        ROUND((curr.adv_value_over_replacement - IFNULL(prev.adv_value_over_replacement, 0)), 2) as vorp_change,
        ROUND((curr.adv_win_shares - IFNULL(prev.adv_win_shares, 0)), 2) as ws_change,
        
        -- Trend Indicators
        CASE 
            WHEN curr.pg_points > IFNULL(prev.pg_points, 0) + 2 THEN 'Significantly Improved'
            WHEN curr.pg_points > IFNULL(prev.pg_points, 0) THEN 'Improved'
            WHEN curr.pg_points < IFNULL(prev.pg_points, 0) - 2 THEN 'Significantly Declined'
            WHEN curr.pg_points < IFNULL(prev.pg_points, 0) THEN 'Declined'
            ELSE 'Stable'
        END as performance_trend,
        
        CASE 
            WHEN curr.adv_value_over_replacement > IFNULL(prev.adv_value_over_replacement, 0) + 1 THEN 'Breakout'
            WHEN curr.adv_value_over_replacement > IFNULL(prev.adv_value_over_replacement, 0) THEN 'Improving'
            WHEN curr.adv_value_over_replacement < IFNULL(prev.adv_value_over_replacement, 0) - 1 THEN 'Regression'
            WHEN curr.adv_value_over_replacement < IFNULL(prev.adv_value_over_replacement, 0) THEN 'Declining'
            ELSE 'Consistent'
        END as impact_trend,
        
        -- Season Year
        CAST(SUBSTRING(curr.season, 1, 4) AS UNSIGNED) as season_year
        
    FROM player_merged_stats curr
    LEFT JOIN player_salaries curr_sal 
        ON curr.player_name = curr_sal.player_name 
        AND curr.season = curr_sal.season
    LEFT JOIN player_merged_stats prev 
        ON curr.player_name = prev.player_name
        AND (
            (curr.season = '2023-24' AND prev.season = '2022-23')
            OR (curr.season = '2024-25' AND prev.season = '2023-24')
        )
    LEFT JOIN player_salaries prev_sal 
        ON prev.player_name = prev_sal.player_name 
        AND prev.season = prev_sal.season
    WHERE curr.games_played >= 20
    ORDER BY curr.season DESC, vorp_change DESC;
    """
    
    logging.info("Fetching player comparison data...")
    df = pd.read_sql(query, connection)
    logging.info(f"Fetched {len(df)} player comparison records")
    
    return df

def create_value_analysis_data(connection):
    """Create comprehensive value analysis data"""
    
    query = """
    SELECT 
        pms.player_name,
        pms.season,
        pms.team,
        pms.position,
        pms.age,
        pms.games_played,
        
        -- Salary
        ps.salary,
        ps.salary_formatted,
        ps.salary_rank,
        
        -- Performance Metrics
        pms.pg_points,
        pms.pg_rebounds,
        pms.pg_assists,
        pms.pg_steals,
        pms.pg_blocks,
        pms.adv_player_efficiency_rating as PER,
        pms.adv_true_shooting_pct as TS_pct,
        pms.adv_usage_pct as usage_pct,
        pms.adv_win_shares as WS,
        pms.adv_value_over_replacement as VORP,
        pms.adv_box_plus_minus as BPM,
        pms.adv_offensive_box_plus_minus as OBPM,
        pms.adv_defensive_box_plus_minus as DBPM,
        
        -- Value Metrics
        CASE 
            WHEN pms.pg_points > 0 AND ps.salary > 0 THEN 
                ROUND(pms.pg_points / (ps.salary / 1000000), 2)
            ELSE NULL 
        END AS points_per_million,
        
        CASE 
            WHEN pms.adv_win_shares > 0 AND ps.salary > 0 THEN 
                ROUND(ps.salary / pms.adv_win_shares, 0)
            ELSE NULL 
        END AS salary_per_WS,
        
        CASE
            WHEN pms.adv_value_over_replacement > 0 AND ps.salary > 0 THEN
                ROUND(ps.salary / pms.adv_value_over_replacement, 0)
            ELSE NULL
        END AS salary_per_VORP,
        
        -- Combined value score (weighted formula)
        CASE 
            WHEN pms.adv_value_over_replacement > 0 AND ps.salary > 0 THEN
                ROUND((pms.adv_value_over_replacement * 1000000) / ps.salary * 100, 2)
            ELSE NULL
        END as value_score,
        
        -- Classifications
        CASE 
            WHEN ps.salary_rank <= 50 AND pms.adv_value_over_replacement >= 4.0 THEN 'Star - Excellent Value'
            WHEN ps.salary_rank <= 50 AND pms.adv_value_over_replacement >= 2.0 THEN 'Star - Fair Value'
            WHEN ps.salary_rank <= 50 AND pms.adv_value_over_replacement < 2.0 THEN 'Star - Overpaid'
            WHEN ps.salary_rank > 50 AND pms.adv_value_over_replacement >= 4.0 THEN 'Hidden Gem'
            WHEN ps.salary_rank > 50 AND pms.adv_value_over_replacement >= 2.0 THEN 'Good Value'
            WHEN ps.salary_rank > 50 AND pms.adv_value_over_replacement >= 0 THEN 'Fair Value'
            ELSE 'Below Average'
        END as value_category,
        
        -- Season Year
        CAST(SUBSTRING(pms.season, 1, 4) AS UNSIGNED) as season_year
        
    FROM player_merged_stats pms
    LEFT JOIN player_salaries ps 
        ON pms.player_name = ps.player_name 
        AND pms.season = ps.season
    WHERE pms.games_played >= 20 AND ps.salary IS NOT NULL
    ORDER BY pms.season DESC, value_score DESC;
    """
    
    logging.info("Fetching value analysis data...")
    df = pd.read_sql(query, connection)
    logging.info(f"Fetched {len(df)} value analysis records")
    
    return df

def create_position_analysis_data(connection):
    """Create position-based analysis data"""
    
    query = """
    SELECT 
        pms.position,
        pms.season,
        COUNT(DISTINCT pms.player_name) as player_count,
        
        -- Salary Stats
        ROUND(AVG(ps.salary), 0) as avg_salary,
        MAX(ps.salary) as max_salary,
        MIN(ps.salary) as min_salary,
        
        -- Performance Stats
        ROUND(AVG(pms.pg_points), 2) as avg_ppg,
        ROUND(AVG(pms.pg_rebounds), 2) as avg_rpg,
        ROUND(AVG(pms.pg_assists), 2) as avg_apg,
        ROUND(AVG(pms.pg_steals), 2) as avg_spg,
        ROUND(AVG(pms.pg_blocks), 2) as avg_bpg,
        
        -- Efficiency Stats
        ROUND(AVG(pms.adv_true_shooting_pct), 3) as avg_TS_pct,
        ROUND(AVG(pms.adv_player_efficiency_rating), 2) as avg_PER,
        ROUND(AVG(pms.adv_usage_pct), 2) as avg_usage_pct,
        
        -- Impact Stats
        ROUND(AVG(pms.adv_win_shares), 2) as avg_WS,
        ROUND(AVG(pms.adv_value_over_replacement), 2) as avg_VORP,
        ROUND(AVG(pms.adv_box_plus_minus), 2) as avg_BPM,
        
        -- Season Year
        CAST(SUBSTRING(pms.season, 1, 4) AS UNSIGNED) as season_year
        
    FROM player_merged_stats pms
    LEFT JOIN player_salaries ps 
        ON pms.player_name = ps.player_name 
        AND pms.season = ps.season
    WHERE pms.games_played >= 40 AND pms.position IS NOT NULL
    GROUP BY pms.position, pms.season
    ORDER BY pms.season DESC, avg_VORP DESC;
    """
    
    logging.info("Fetching position analysis data...")
    df = pd.read_sql(query, connection)
    logging.info(f"Fetched {len(df)} position analysis records")
    
    return df

def main():
    """Main execution function"""
    
    # Connect to database
    connection = connect_to_database(DB_CONFIG)
    if not connection:
        logging.error("Failed to connect to database")
        return
    
    try:
        # Create datasets
        logging.info("=" * 60)
        logging.info("Creating Tableau-ready datasets from merged stats...")
        logging.info("=" * 60)
        
        # 1. Main dashboard data
        main_df = create_main_dashboard_data(connection)
        main_df.to_csv('tableau_main_dashboard.csv', index=False)
        logging.info("✓ Created: tableau_main_dashboard.csv")
        
        # 2. Team summary data
        team_df = create_team_summary_data(connection)
        team_df.to_csv('tableau_team_summary.csv', index=False)
        logging.info("✓ Created: tableau_team_summary.csv")
        
        # 3. Player comparison data
        comparison_df = create_player_comparison_data(connection)
        comparison_df.to_csv('tableau_player_comparison.csv', index=False)
        logging.info("✓ Created: tableau_player_comparison.csv")
        
        # 4. Value analysis data
        value_df = create_value_analysis_data(connection)
        value_df.to_csv('tableau_value_analysis.csv', index=False)
        logging.info("✓ Created: tableau_value_analysis.csv")
        
        # 5. Position analysis data
        position_df = create_position_analysis_data(connection)
        position_df.to_csv('tableau_position_analysis.csv', index=False)
        logging.info("✓ Created: tableau_position_analysis.csv")
        
        # Print summary
        logging.info("=" * 60)
        logging.info("TABLEAU DATA PREPARATION COMPLETE!")
        logging.info("=" * 60)
        logging.info("\nDatasets created:")
        logging.info(f"  1. tableau_main_dashboard.csv      - {len(main_df):,} records")
        logging.info(f"  2. tableau_team_summary.csv        - {len(team_df):,} records")
        logging.info(f"  3. tableau_player_comparison.csv   - {len(comparison_df):,} records")
        logging.info(f"  4. tableau_value_analysis.csv      - {len(value_df):,} records")
        logging.info(f"  5. tableau_position_analysis.csv   - {len(position_df):,} records")
        logging.info("\nThese files are ready to import into Tableau!")
        logging.info("\nNew features in this update:")
        logging.info("  • Complete advanced metrics (PER, TS%, VORP, BPM, WS)")
        logging.info("  • Win shares and impact metrics")
        logging.info("  • Defensive and offensive ratings")
        logging.info("  • Award tracking and All-Star status")
        logging.info("  • Comprehensive value calculations")
        
    except Exception as e:
        logging.error(f"Error creating datasets: {e}")
        import traceback
        logging.error(traceback.format_exc())
        
    finally:
        if connection and connection.is_connected():
            connection.close()
            logging.info("\nDatabase connection closed")

if __name__ == "__main__":
    main()


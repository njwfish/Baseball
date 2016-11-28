import MySQLdb
from MySQL_Utils import query_sql, insert_sql
import os
import os.path
from csv import reader

cols=['date','game_num','day','away','a_league','a_game_num','home','h_league','h_game_num','a_score','h_score','len_outs','dn','Completion','Forfeit','Protest','park_id','attendance','len_min','a_line','h_line','h_ab','h_h','h_2b','h_3b','h_hr','h_rbi','h_sh','h_sf','h_hbp','h_bb','h_ibb','h_k','h_sb','h_cs','h_gdp','h_ci','h_lob','h_num_pitchers','h_ier','h_er','h_wp','h_bk','h_po','h_a','h_e','h_pb','h_dp','h_tp','a_ab','a_h','a_2b','a_3b','a_hr','a_rbi','a_sh','a_sf','a_hbp','a_bb','a_ibb','a_k','a_sb','a_cs','a_gdp','a_ci','a_lob','a_num_pitchers','a_ier','a_er','a_wp','a_bk','a_po','a_a','a_e','a_pb','a_dp','a_tp','h_ump_id','h_ump','1b_ump_id','1b_ump','2b_ump_id','2b_ump','3b_ump_id','3b_ump','lf_ump_id','lf_ump','rf_ump_id','rf_ump','a_manager_id','a_manager','h_manager_id','h_manager','w_pitcher_id','w_pitcher','l_pitcher_id','l_pitcher','s_pitcher_id','s_pitcher','w_batter_id','w_batter','h_start_pitcher_id','h_start_pitcher','a_start_pitcher_id','a_start_pitcher','h_1_id','h_1','h_1_pos','h_2_id','h_2','h_2_pos','h_3_id','h_3','h_3_pos','h_4_id','h_4','h_4_pos','h_5_id','h_5','h_5_pos','h_6_id','h_6','h_6_pos','h_7_id','h_7','h_7_pos','h_8_id','h_8','h_8_pos','h_9_id','h_9','h_9_pos','a_1_id','a_1','a_1_pos','a_2_id','a_2','a_2_pos','a_3_id','a_3','a_3_pos','a_4_id','a_4','a_4_pos','a_5_id','a_5','a_5_pos','a_6_id','a_6','a_6_pos','a_7_id','a_7','a_7_pos','a_8_id','a_8','a_8_pos','a_9_id','a_9','a_9_pos','additional_info','acquistional_info']
db = MySQLdb.connect("localhost", "root", "12345", "gamelogs")
cursor = db.cursor()

for path, subdirs, files in os.walk("."):
    for file in [f for f in files if f.endswith(".TXT")]:
        with open(os.path.join(path, file), 'r') as gamelog:
            for vals in reader(gamelog):
            	for i in range(1,len(cols)):
            		if vals[i] == '' or vals[i] == '(none)' or "'" in vals[i] or vals[i] < 0:
            			vals[i] = 'NULL'
            		elif '_id' in cols[i] and i > 20:
						vals[i] = hash(vals[i])
                insert_sql('gamelogsNumID',cols,vals,db,cursor)
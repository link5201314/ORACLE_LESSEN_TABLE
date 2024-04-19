create or replace PROCEDURE TRW.USP_MOVE_IS_OLD_DATA
AS
    cur_sel_LESSEN_TABLE sys_refcursor ;
    sql_list TRW.LESSEN_TABLE@TRWP101_TC%ROWTYPE;

    v_count NUMBER;
	v_new_job_id NUMBER;
	v_user_id VARCHAR2(30); v_sess_id NUMBER(10); v_client_ip VARCHAR2(20); v_client_name VARCHAR2(150); v_client_machine VARCHAR2(150);

    v_table_name VARCHAR2(50);
    v_sys_owner VARCHAR2(10);
    v_is_enable CHAR(1);

    v_job_id_cnt NUMBER;
    v_move_job_id NUMBER;
    v_sql_text VARCHAR2(200);

    v_prod_last_cnt NUMBER;
    v_dr_last_cnt NUMBER;

    v_bak_cnt NUMBER;
    v_his_cnt NUMBER;
    v_del_cnt NUMBER;
	
	v_funcstart TIMESTAMP := SYSTIMESTAMP;
    v_timestart TIMESTAMP; v_timeend TIMESTAMP; v_elapsed INTERVAL DAY(2) TO SECOND(3);
	v_lv2_error_occurred BOOLEAN := FALSE; v_lv3_error_occurred BOOLEAN := FALSE; v_haserror BOOLEAN := FALSE; v_not_found_error BOOLEAN := TRUE;
	v_err_code NUMBER; v_err_msg VARCHAR2(4000);
	para_degree NUMBER:= 16;
BEGIN
	--DBMS_OUTPUT.ENABLE (buffer_size => NULL); --使DBMS_OUTPUT的buff_size無限制
	DBMS_OUTPUT.ENABLE (1000000); --使DBMS_OUTPUT的buff_size為1000000

	select USER, SYS_CONTEXT('USERENV', 'SID'), SYS_CONTEXT('USERENV','IP_ADDRESS'), SYS_CONTEXT('USERENV','OS_USER'), SYS_CONTEXT('USERENV','HOST')
    into v_user_id,v_sess_id,v_client_ip,v_client_name,v_client_machine
    from dual;
	--1) 為此次job取得最新的job_id
	SELECT TRW.LESSEN_TABLE_MOVE_SQ.NEXTVAL INTO v_new_job_id FROM dual;
	
    DBMS_OUTPUT.PUT_LINE('v_new_job_id='|| v_new_job_id ||'; USER_ID=' || v_user_id || '; SESS_ID=' || v_sess_id || '; CLIENT_IP=' || v_client_ip 
	|| '; CLIENT_NAME=' || v_client_name || '; CLIENT_MACHINE=' || v_client_machine);
	
	INSERT INTO LESSEN_TABLE_MOVE_JOB 
    VALUES(v_new_job_id, 'RUNNING', 'USP_MOVE_IS_OLD_DATA', v_funcstart, null
	, v_user_id, v_sess_id, v_client_ip , v_client_name, v_client_machine, NULL, NULL);
    commit;

    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE TRW.LESSEN_TABLE';
    EXCEPTION
        WHEN OTHERS THEN
        -- suppresses "table or view does not exist" exception
        IF SQLCODE = -942 THEN
            NULL; 
        END IF;
    END;


    EXECUTE IMMEDIATE 'CREATE TABLE TRW.LESSEN_TABLE as Select * from TRW.LESSEN_TABLE@TRWP101_TC';

    open cur_sel_LESSEN_TABLE for Select * from TRW.LESSEN_TABLE@TRWP101_TC;

	v_lv2_error_occurred := FALSE;
	--[迴圈開始]逐一處理瘦身table
    Loop
		BEGIN
			FETCH cur_sel_LESSEN_TABLE INTO sql_list;
			EXIT WHEN cur_sel_LESSEN_TABLE%NOTFOUND;
			v_not_found_error := FALSE;

			v_table_name := sql_list.PK_LESSEN_TABLE;
			v_is_enable := sql_list.ENABLE;
			v_sys_owner := sql_list.SYS_OWNER;

			DBMS_OUTPUT.PUT_LINE('Processing table_name=' || v_table_name || '; SYS_OWNER=' || v_sys_owner || '; IS_ENABLE=' || v_is_enable);
			--2) 確認上一季資料已完整搬移再清空table_bakhis：
			v_sql_text := 'SELECT count(DISTINCT move_job_id) FROM TRW.' || v_table_name || '_bakhis@TRWP101_TC';
			EXECUTE IMMEDIATE v_sql_text INTO v_job_id_cnt;

			--判斷move_job_id的值是否唯一
			IF v_job_id_cnt = 1 THEN
			  DBMS_OUTPUT.PUT_LINE('found');
			  -- 取得該筆 move_job_id 的值
			  v_sql_text := 'SELECT move_job_id FROM TRW.' || v_table_name || '_bakhis@TRWP101_TC fetch first row only';
			  EXECUTE IMMEDIATE v_sql_text INTO v_move_job_id;
			  DBMS_OUTPUT.PUT_LINE('get job id: ' || v_move_job_id);

			  --取得主中心與異地相同job_id的資料筆數
			  v_sql_text := 'SELECT count(*) FROM TRW.' || v_table_name || '_bakhis@TRWP101_TC where move_job_id = ' || v_move_job_id;
			  EXECUTE IMMEDIATE v_sql_text INTO v_prod_last_cnt;

			  v_sql_text := 'SELECT count(*) FROM HIS.' || v_table_name || ' where move_job_id = ' || v_move_job_id;
			  EXECUTE IMMEDIATE v_sql_text INTO v_dr_last_cnt;

			  IF v_prod_last_cnt != v_dr_last_cnt THEN
				RAISE_APPLICATION_ERROR(-20005, '[' || v_table_name || ', MOVE_JOB_ID= '|| v_move_job_id || ']: 主中心與異地his表筆數不一致!');
			  ELSE
				DBMS_OUTPUT.PUT_LINE('主中心與異地his表筆數一致，Truncate主中心his表');
				--v_sql_text := 'USP_TRUNC_BAKHIS_TBL@TRWP101_TC(:v_table_name);';
				v_sql_text := 'BEGIN USP_TRUNC_BAKHIS_TBL@TRWP101_TC(''' || v_table_name || '''); END;';  --一定要加begin & end
				DBMS_OUTPUT.PUT_LINE(v_sql_text);

				EXECUTE IMMEDIATE v_sql_text ;
				--EXECUTE IMMEDIATE v_sql_text USING v_table_name;
			  END IF;
			ELSE
				-- 沒有資料或資料筆數大於一筆
				DBMS_OUTPUT.PUT_LINE('No data or more than one row found.');
				RAISE_APPLICATION_ERROR(-20004, '[TRW.' || v_table_name || '_bakhis@TRWP101_TC]，' || 'MOVE_JOB_ID值不唯一!');
			END IF;

			INSERT INTO LESSEN_TABLE_MOVE_SQL 
			VALUES(v_new_job_id, 'RUNNING', v_table_name, v_sys_owner, v_is_enable
			, v_sql_text, NULL, NULL);
			commit;

			--開始備份與刪除歷史資料
			--要測試有沒有辦法進行parallel加速
			v_lv3_error_occurred := FALSE;
			EXECUTE IMMEDIATE 'Alter session force parallel DML PARALLEL ' || para_degree;
			BEGIN
				v_timestart := SYSTIMESTAMP;
				SAVEPOINT DOHIS;	
				DBMS_OUTPUT.PUT_LINE('Start Delete His Data!');
				
				--要測試有沒有辦法進行parallel加速
				v_sql_text := 'INSERT INTO TRW.' || v_table_name || '_bakhis@TRWP101_TC select t.*, ' || v_new_job_id || ' FROM TRW.' || v_table_name || '@TRWP101_TC t WHERE t.is_old_data = ''Y''';
				DBMS_OUTPUT.PUT_LINE(v_sql_text);
				
				EXECUTE IMMEDIATE v_sql_text;
				v_bak_cnt := SQL%ROWCOUNT;
				DBMS_OUTPUT.PUT_LINE(v_bak_cnt);
				
				v_sql_text := 'INSERT INTO HIS.' || v_table_name || ' select t.*, ' || v_new_job_id || ' FROM TRW.' || v_table_name || ' t WHERE t.is_old_data = ''Y''';
				DBMS_OUTPUT.PUT_LINE(v_sql_text);
				
				EXECUTE IMMEDIATE v_sql_text;
				v_his_cnt := SQL%ROWCOUNT;
				DBMS_OUTPUT.PUT_LINE(v_his_cnt);
				
				--要測試有沒有辦法進行parallel加速
				v_sql_text := 'DELETE FROM TRW.' || v_table_name || '@TRWP101_TC t WHERE t.is_old_data = ''Y''';
				DBMS_OUTPUT.PUT_LINE(v_sql_text);
				
				EXECUTE IMMEDIATE v_sql_text;
				v_del_cnt := SQL%ROWCOUNT;
				DBMS_OUTPUT.PUT_LINE(v_his_cnt);
				
				v_timeend := SYSTIMESTAMP;
                v_elapsed := v_timeend-v_timestart;
				
				--v_del_cnt := 0;
				
				IF (v_bak_cnt != v_his_cnt) or (v_bak_cnt != v_del_cnt) THEN
					RAISE_APPLICATION_ERROR(-20006, '[' || v_table_name || ', MOVE_JOB_ID= '|| v_move_job_id || ']: 備份與刪除筆數不一致!');
				END IF;

				--執行無例外錯誤最後才commit，並將此job執行紀錄至job紀錄table(含job_id、處理table、備份筆數、日期)
				DBMS_OUTPUT.PUT_LINE('Execute Succeed!');
				INSERT INTO LESSEN_TABLE_MOVE_SQL_DETAIL VALUES(v_new_job_id, v_table_name, v_sql_text, v_timestart, v_timeend, v_elapsed, v_bak_cnt, 0, NULL);
				commit;
			EXCEPTION
				WHEN OTHERS THEN
				v_lv3_error_occurred := TRUE;
				v_haserror := TRUE;
				v_err_code := SQLCODE;
				v_err_msg := SQLERRM;
				dbms_output.put_line( '[例外處理3]SQLCODE : ['||SQLCODE||']' );
				dbms_output.put_line( '[例外處理3]SQLERRM : ['||SQLERRM||']' );
				ROLLBACK TO DOHIS;
				INSERT INTO LESSEN_TABLE_MOVE_SQL_DETAIL VALUES(v_new_job_id, v_table_name, v_sql_text, v_timestart, SYSTIMESTAMP
				, SYSTIMESTAMP-v_timestart, 0, v_err_code, v_err_msg);
				commit;
			END;
			
			commit;
			EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
			
			IF v_lv3_error_occurred  THEN
				UPDATE LESSEN_TABLE_MOVE_SQL SET STATUS = 'WARNING', SQL_CODE=0 WHERE JOB_ID = v_new_job_id AND LESSEN_TABLE = v_table_name;
			ELSE
				UPDATE LESSEN_TABLE_MOVE_SQL SET STATUS = 'COMPLETE', SQL_CODE=0 WHERE JOB_ID = v_new_job_id AND LESSEN_TABLE = v_table_name;
			END IF;
			commit;
		EXCEPTION
			WHEN OTHERS THEN
			  v_lv2_error_occurred := TRUE;
			  v_haserror := TRUE;
			  v_err_code := SQLCODE;
			  v_err_msg := SQLERRM;
			  ROLLBACK;
			  UPDATE LESSEN_TABLE_MOVE_SQL SET STATUS='ERROR', SQL_CODE=v_err_code, SQL_ERRM=v_err_msg  
			  WHERE JOB_ID = v_new_job_id AND LESSEN_TABLE = v_table_name;
			  commit;
			  dbms_output.put_line( '[例外處理2]SQLCODE : ['||SQLCODE||']' );
			  dbms_output.put_line( '[例外處理2]SQLERRM : ['||SQLERRM||']' );
		END;
	END LOOP;

    dbms_output.put_line('Total Rows: ' || cur_sel_LESSEN_TABLE%rowcount);--here you will get total row count;

    CLOSE cur_sel_LESSEN_TABLE;
	
	IF v_not_found_error THEN
		RAISE_APPLICATION_ERROR(-20002, '在LESSEN_TABLE中沒有任何已定義項目!');
	END IF;
	
    IF v_lv2_error_occurred or v_haserror THEN
        UPDATE LESSEN_TABLE_MOVE_JOB SET STATUS='WARNING', SQL_CODE=0, FUNC_END_TIME=SYSTIMESTAMP WHERE JOB_ID = v_new_job_id;
    ELSE
        UPDATE LESSEN_TABLE_MOVE_JOB SET STATUS='COMPLETE', SQL_CODE=0, FUNC_END_TIME=SYSTIMESTAMP WHERE JOB_ID = v_new_job_id;
    END IF;
	commit;
EXCEPTION
   WHEN OTHERS THEN
      v_err_code := SQLCODE;
      v_err_msg := SQLERRM;
	  ROLLBACK;
      UPDATE LESSEN_TABLE_MOVE_JOB SET STATUS='ERROR', FUNC_END_TIME=SYSTIMESTAMP, SQL_CODE=v_err_code, SQL_ERRM=v_err_msg  
	  WHERE JOB_ID = v_new_job_id;
      commit;
      dbms_output.put_line( '[例外處理1]SQLCODE : ['||SQLCODE||']' );
      dbms_output.put_line( '[例外處理1]SQLERRM : ['||SQLERRM||']' );
END;
/

DROP PROCEDURE USP_MOVE_IS_OLD_DATA;
DROP PUBLIC SYNONYM USP_MOVE_IS_OLD_DATA;

CREATE PUBLIC SYNONYM USP_MOVE_IS_OLD_DATA FOR TRW.USP_MOVE_IS_OLD_DATA;

EXEC USP_MOVE_IS_OLD_DATA();

update SSP_TKT_TXN_HIS_BAKHIS set move_job_id = '2';
update SSP_TKT_TXN_HIS_BAKHIS set move_job_id = '3' where PK_SSP_TKT_TXN_HIS = '608D53961BB847A7BD645D8FBCBB1E37';

commit;

select * from LESSEN_TABLE_MOVE_JOB;
select * from LESSEN_TABLE_MOVE_SQL;
select * from LESSEN_TABLE_MOVE_SQL_DETAIL;


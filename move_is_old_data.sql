create or replace PROCEDURE TRW.USP_MOVE_IS_OLD_DATA
AS
    cur_sel_LESSEN_TABLE sys_refcursor ;
    sql_list TRW.LESSEN_TABLE@TRWP101_TC%ROWTYPE;

    v_count NUMBER;
	v_new_job_id NUMBER;

    v_table_name VARCHAR2(50);
    v_sys_owner VARCHAR2(10);
    v_is_enable CHAR(1);

    v_job_id_cnt NUMBER;
    v_move_job_id NUMBER;
    v_sql_text VARCHAR2(100);

    v_prod_last_cnt NUMBER;
    v_dr_last_cnt NUMBER;

    v_bak_cnt NUMBER;
    v_his_cnt NUMBER;
    v_del_cnt NUMBER;

BEGIN
	--1) 為此次job取得最新的job_id
	SELECT TRW.LESSEN_TABLE_MOVE_JOB_ID_SQ.NEXTVAL INTO v_new_job_id FROM dual;
	dbms_output.put_line('v_new_job_id: ' || v_new_job_id);

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

	--[迴圈開始]逐一處理瘦身table
    Loop
        FETCH cur_sel_LESSEN_TABLE INTO sql_list;
        EXIT WHEN cur_sel_LESSEN_TABLE%NOTFOUND;

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
            RAISE_APPLICATION_ERROR(-20005, '[' || v_table_name || ']:' || 'MOVE_JOB_ID= '|| v_move_job_id ||'，主中心與異地his表筆數不一致!');
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

        --開始刪除歷史資料
        BEGIN
            SAVEPOINT DOHIS;	
            DBMS_OUTPUT.PUT_LINE('Start Delete His Data!');

            --執行無例外錯誤最後才commit，並將此job執行紀錄至job紀錄table(含job_id、處理table、備份筆數、日期)
            commit;
        EXCEPTION
            WHEN OTHERS THEN
            --  v_lv3_error_occurred := TRUE;
            --  v_haserror := TRUE;
            --  v_err_code := SQLCODE;
            --  v_err_msg := SQLERRM;
            --  INSERT INTO LESSEN_TABLE_EXEC_SQL_DETAIL VALUES(v_job_id, v_table_name, l_counter, v_timestart, SYSTIMESTAMP
            --  , SYSTIMESTAMP-v_timestart, v_row, v_sql_para, v_err_code, v_err_msg);
            --  commit;
                dbms_output.put_line( '[例外處理3]SQLCODE : ['||SQLCODE||']' );
                dbms_output.put_line( '[例外處理3]SQLERRM : ['||SQLERRM||']' );
                rollback TO DOHIS;
        END;
        
        commit;
	END LOOP;

    dbms_output.put_line('Total Rows: ' || cur_sel_LESSEN_TABLE%rowcount);--here you will get total row count;

    CLOSE cur_sel_LESSEN_TABLE;
END;
/

DROP PROCEDURE USP_MOVE_IS_OLD_DATA;
DROP PUBLIC SYNONYM USP_MOVE_IS_OLD_DATA;

CREATE PUBLIC SYNONYM USP_MOVE_IS_OLD_DATA FOR TRW.USP_MOVE_IS_OLD_DATA;


update SSP_TKT_TXN_HIS_BAKHIS set move_job_id = '2';
update SSP_TKT_TXN_HIS_BAKHIS set move_job_id = '3' where PK_SSP_TKT_TXN_HIS = '608D53961BB847A7BD645D8FBCBB1E37';

commit;




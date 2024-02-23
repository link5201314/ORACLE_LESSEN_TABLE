create or replace PROCEDURE TRW.USP_MOVE_IS_OLD_DATA
AS
    cur_sel_LESSEN_TABLE sys_refcursor ;
    
    v_count NUMBER;
	v_new_job_id NUMBER;
    
    v_table_name VARCHAR2(50);
    v_sys_owner VARCHAR2(10);
    v_is_enable CHAR(1);
    
    sql_list TRW.LESSEN_TABLE@TRWP101_TC%ROWTYPE;
    
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
		
	END LOOP;
    
    dbms_output.put_line('Total Rows: ' || cur_sel_LESSEN_TABLE%rowcount);--here you will get total row count;
    
    CLOSE cur_sel_LESSEN_TABLE;
END;

DROP PROCEDURE USP_MOVE_IS_OLD_DATA;
DROP PUBLIC SYNONYM USP_MOVE_IS_OLD_DATA;

CREATE PUBLIC SYNONYM USP_MOVE_IS_OLD_DATA FOR TRW.USP_MOVE_IS_OLD_DATA;


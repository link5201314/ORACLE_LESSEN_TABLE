--注意DB Link @TRWP101_TC 需替換為對應的DB
drop PROCEDURE TRW.USP_MOVE_IS_OLD_DATA;

CREATE OR REPLACE PROCEDURE TRW.USP_MOVE_IS_OLD_DATA()
AS
	CURSOR cur_sel_LESSEN_TABLE;
	v_new_job_id NUMBER;
BEGIN

	--1) 為此次job取得最新的job_id
	SELECT TRW.LESSEN_TABLE_MOVE_JOB_ID_SQ.NEXTVAL INTO v_new_job_id FROM dual;
	dbms_output.put_line('v_new_job_id: ' || v_new_job_id);
	
	
	CREATE GLOBAL TEMPORARY TABLE temp_data
	ON COMMIT PRESERVE ROWS
	AS
	select * from TRW.LESSEN_TABLE@TRWP101_TC;
	
	cur_sel_LESSEN_TABLE IS select * from temp_data;

	FOR sql_list IN cur_sel_LESSEN_TABLE LOOP
    BEGIN
		v_table_name := sql_list.PK_LESSEN_TABLE;
        v_is_enable := sql_list.ENABLE;
        v_sys_owner := sql_list.SYS_OWNER;
		
		DBMS_OUTPUT.PUT_LINE('Processing table_name=' || v_table_name || '; SYS_OWNER=' || v_sys_owner || '; IS_ENABLE=' || v_is_enable);
		
	END LOOP;
	

	--dbms_output.put_line('Total Rows: ' || l_cur%rowcount);--here you will get total row count
END;
/
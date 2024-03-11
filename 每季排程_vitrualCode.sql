
--每季排程JOB
BEGIN
	--1) 為此次job取得最新的job_id
	new_job_id = LESSEN_TABLE_MOVE_JOB_ID_SQ.nextval;

	--[迴圈開始]逐一處理瘦身table
		--2) 確認上一季資料已完整搬移再清空table_bakhis：
		--取得當前主中心His.Table的jobID
		cousor = select distinct move_job_id from table_bakhis;
		IF cousor%rowcount>1 THEN  --move_job_id在主中心his.table應只有唯一值否則拋出Excption，並rollback;
			raise_exception
		ELSE IF cousor%rowcount>0 THEN
			job_id = cousor.move_job_id

			--取得主中心與異地相同job_id的資料筆數
			prod_last_cnt = select count(*) from table_bakhis where move_job_id = :job_id;
			dr_last_cnt = select count(*) from his.table@dr where move_job_id = :job_id;
			
			--如果判斷筆數不一致則拋出Exception，並rollback
			IF (prod_last_cnt != dr_last_cnt) THEN
			  raise_exception
			ELSE
			  --清除主中心TABLE內的標記舊資料
			  truncate table table_bakhis;
			END IF;
		END IF;
		
		--3) 將主中心Table標記舊資料複製至table_bakhis
		bak_cnt = insert into table_bakhis select *, new_job_id  from table where is_old_data = Y;

		--4) 將主中心Table標記舊資料複製至HIS.TABLE
		his_cnt = insert into his.table@dr select *, new_job_id  from table@dr where is_old_data = Y;
		
		--5) 刪除主中心TABLE內的標記舊資料(異地不用刪除，會透過OGG同步)
		del_cnt = delete from table where is_old_data = Y;


		--刪除的筆數要等於備份的筆數否則拋出Excption，並rollback，同時輸出job失敗status code至檔案，給BMC進行告警;
		IF (bak_cnt != del_cnt) THEN
			raise_exception
		END IF;
		
		--執行無例外錯誤最後才commit，並將此job執行紀錄至job紀錄table(含job_id、處理table、備份筆數、日期)
		commit;
EXCEPTION
	rollback;
END;


主中心
prod.table -> [prod]table_bakhis(增加move_job_id欄位)
create table <table_name> as
select *
  from <source_table> 
 where 1 = 0;


ALTER TABLE table_bakhis ADD (move_job_id Number);

create table SSP_TKT_TXN_HIS_BAKHIS as
select *
  from SSP_TKT_TXN_HIS
 where 1 = 0;
 
ALTER TABLE SSP_TKT_TXN_HIS_BAKHIS ADD (move_job_id Number);

異地
dr.table -> [dr]his.table(增加move_job_id欄位)
ALTER TABLE his.table@dr ADD (move_job_id Number);





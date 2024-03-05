CREATE OR REPLACE TRW.PROCEDURE USP_TRUNC_BAKHIS_TBL(
    in_tablename VARCHAR2
)
AS
v_trunc_tbl_sql_pre_text VARCHAR2(100);
v_trunc_tbl_sql_full_text VARCHAR2(150);
BEGIN   
    v_trunc_tbl_sql_pre_text := 'TRUNCATE TABLE TRW.';
    v_trunc_tbl_sql_full_text := v_trunc_tbl_sql_pre_text || in_tablename || '_BAKHIS';
    DBMS_OUTPUT.PUT_LINE('v_trunc_tbl_sql_full_text: ' || v_trunc_tbl_sql_full_text);
    EXECUTE IMMEDIATE v_trunc_tbl_sql_full_text;
END;
/

CREATE PUBLIC SYNONYM USP_TRUNC_BAKHIS_TBL FOR TRW.USP_TRUNC_BAKHIS_TBL;
GRANT EXECUTE ON USP_TRUNC_BAKHIS_TBL TO DBLINK;



CREATE TABLE trw.persons_BAKHIS(
    person_id NUMBER,
    first_name VARCHAR2(50) NOT NULL,
    last_name VARCHAR2(50) NOT NULL,
    PRIMARY KEY(person_id)
);

insert into trw.persons_BAKHIS values(1,'isaac', 'liu');
commit;

CREATE PUBLIC SYNONYM persons_BAKHIS FOR TRW.persons_BAKHIS;

GRANT SELECT, INSERT, UPDATE, DELETE ON TRW.persons_BAKHIS TO AP_ROLE; 
GRANT SELECT ON TRW.persons_BAKHIS TO SEL_ROLE; 

EXEC TRW.USP_TRUNC_BAKHIS_TBL('PERSONS');

--異地透過DB link調用
EXEC USP_TRUNC_BAKHIS_TBL@TRWP101_TC('PERSONS');
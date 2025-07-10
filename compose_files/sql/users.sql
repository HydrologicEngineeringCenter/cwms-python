set define on
define OFFICE_EROC=&1
begin
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','All Users', 'HQ');
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','All Users', 'SPK');
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','CWMS Users', 'HQ');
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','CWMS User Admins', 'HQ');
    

    cwms_sec.add_cwms_user('l2hectest',NULL,'SPK');
    cwms_sec.update_edipi('l2hectest',1234567890);
    cwms_sec.add_user_to_group('l2hectest','All Users', 'SPK');
    cwms_sec.add_user_to_group('l2hectest','CWMS Users', 'SPK');
    cwms_sec.add_user_to_group('l2hectest','TS ID Creator','SPK');
    cwms_sec.add_user_to_group('l2hectest','CWMS User Admins', 'SPK');

    cwms_sec.add_cwms_user('l1hectest',NULL,'SPL');
    -- intentionally no extra permissions.
    --cwms_sec.add_user_to_group('l2hectest','CWMS Users', 'SPL');

    cwms_sec.add_cwms_user('m5hectest',NULL,'SWT');
    cwms_sec.add_user_to_group('m5hectest','All Users', 'SWT');
    cwms_sec.add_user_to_group('m5hectest','CWMS Users', 'SWT');
    execute immediate 'grant execute on cwms_20.cwms_upass to web_user';


    cwms_sec.add_cwms_user('m5testadmin', NULL, 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','All Users', 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','CWMS Users', 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','CWMS User Admins', 'LRL');

    cwms_sec.add_cwms_user('q0hectest', NULL, 'LRL');
    cwms_sec.add_user_to_group('q0hectest','All Users', 'LRL');
    cwms_sec.add_user_to_group('q0hectest','CWMS Users', 'LRL');
    cwms_sec.add_user_to_group('q0hectest','TS ID Creator','LRL');

    cwms_sec.add_cwms_user('q0hectest', NULL, 'SPK');
    cwms_sec.add_user_to_group('q0hectest','All Users', 'SPK');
    cwms_sec.add_user_to_group('q0hectest','CWMS Users', 'SPK');
    cwms_sec.add_user_to_group('q0hectest','TS ID Creator','SPK');

    cwms_sec.add_cwms_user('q0hectest', NULL, 'MVP');
    cwms_sec.add_user_to_group('q0hectest','All Users', 'MVP');
    cwms_sec.add_user_to_group('q0hectest','CWMS Users', 'MVP');
    cwms_sec.add_user_to_group('q0hectest','TS ID Creator','MVP');

    insert into cwms_20.at_api_keys (userid, key_name, apikey) values ('Q0HECTEST', 'testkey', '0123456789abcdef0123456789abcdef');

    insert into cwms_20.at_api_keys (userid, key_name, apikey) values ('L2HECTEST', 'testkey2', '1234567890abcdef1234567890abcdef');

end;
/
quit;
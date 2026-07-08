set define on
define OFFICE_EROC=&1
begin
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','All Users', 'HQ');
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','All Users', 'SPK');
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','All Users', 'MVP');
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','All Users', 'LRL');
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','CWMS Users', 'HQ');
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','CWMS User Admins', 'HQ');
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','CWMS PD Users', 'HQ');

    cwms_sec.add_cwms_user('l2hectest',NULL,'SPK');
    cwms_sec.update_edipi('l2hectest',1234567890);
    cwms_sec.add_user_to_group('l2hectest','All Users', 'SPK');
    cwms_sec.add_user_to_group('l2hectest','CWMS Users', 'SPK');
    cwms_sec.add_user_to_group('l2hectest','TS ID Creator','SPK');
    cwms_sec.add_user_to_group('l2hectest','CWMS User Admins', 'SPK');
    cwms_sec.add_user_to_group('l2hectest', 'CWMS PD Users', 'SPK');

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
    cwms_sec.add_user_to_group('m5testadmin','CWMS PD Users', 'LRL');

    cwms_sec.add_cwms_user('q0hectest', NULL, 'LRL');
    cwms_sec.add_user_to_group('q0hectest','All Users', 'LRL');
    cwms_sec.add_user_to_group('q0hectest','CWMS Users', 'LRL');
    cwms_sec.add_user_to_group('q0hectest','TS ID Creator','LRL');
    cwms_sec.add_user_to_group('q0hectest','CWMS User Admins', 'LRL');
    cwms_sec.add_user_to_group('q0hectest','CWMS PD Users', 'LRL');

    cwms_sec.add_cwms_user('q0hectest', NULL, 'SPK');
    cwms_sec.add_user_to_group('q0hectest','All Users', 'SPK');
    cwms_sec.add_user_to_group('q0hectest','CWMS Users', 'SPK');
    cwms_sec.add_user_to_group('q0hectest','TS ID Creator','SPK');
    cwms_sec.add_user_to_group('q0hectest','CWMS User Admins', 'SPK');
    cwms_sec.add_user_to_group('q0hectest','CWMS PD Users', 'SPK');

    cwms_sec.add_cwms_user('q0hectest', NULL, 'MVP');
    cwms_sec.add_user_to_group('q0hectest','All Users', 'MVP');
    cwms_sec.add_user_to_group('q0hectest','CWMS Users', 'MVP');
    cwms_sec.add_user_to_group('q0hectest','TS ID Creator','MVP');
    cwms_sec.add_user_to_group('q0hectest','CWMS User Admins', 'MVP');
    cwms_sec.add_user_to_group('q0hectest','CWMS PD Users', 'MVP');

    -- Single-office user for the general test API key. Bound to a dedicated
    -- single-office user (not Q0HECTEST) to work around a CDA bug where the
    -- CWMS_UTIL.user_office_id fallback raises ORA-01422 for multi-office
    -- users when SESSION_OFFICE_ID context is unset on a request.
    cwms_sec.add_cwms_user('pytest_mvp_admin', NULL, 'MVP');
    cwms_sec.add_user_to_group('pytest_mvp_admin','All Users', 'MVP');
    cwms_sec.add_user_to_group('pytest_mvp_admin','CWMS Users', 'MVP');
    cwms_sec.add_user_to_group('pytest_mvp_admin','TS ID Creator','MVP');
    cwms_sec.add_user_to_group('pytest_mvp_admin','CWMS User Admins', 'MVP');
    cwms_sec.add_user_to_group('pytest_mvp_admin','CWMS PD Users', 'MVP');

    insert into cwms_20.at_api_keys (userid, key_name, apikey, created, expires) values ('PYTEST_MVP_ADMIN', 'testkey', 'ak1_CYflBX6c$argon2id$v=19$m=19456,t=2,p=1$8Wh8X9m+O81UvrbCJ/eOFQ$+E0Rp3jhjduIHxaqmzx+OLR43B3HdcMuDyn8cO5/69s', sysdate, sysdate + 365);
    -- key is  ak1_CYflBX6cQOHlJkkcsA6NPvJ7npm1kynzfUsa45ncIPcGNewkcvK2ounQN8MaDj8Wkc8o0HiZvLkETpGrGkl3OvJD9Nt0vQCIPLBeqQiLGBQHsPDZmk1gEkVCzubSyfKy31bagcf0jrajn6zCcRAv1tpMpnucFCkUwCpTYwNCfCnPkqukNVpOyTv7I2II8NIxBQmQOZPc09yOrKPkQpj1sHM4NNxIcUfTZrPpidT1QGjhfVaaWW1AiqodkxXPxlTqvuRLz9bL
    
    insert into cwms_20.at_api_keys (userid, key_name, apikey, created, expires) values ('L2HECTEST', 'testkey2', 'ak1_3rF3RXlB$argon2id$v=19$m=19456,t=2,p=1$xxL2ItUkn3gC1LT5F8Wb0g$2az8A0GpJVVhbaccD4ICVWvnM2uoKzU652r9jemE9qg', sysdate, sysdate + 365);
    -- key is   ak1_3rF3RXlBRpWiWNdBhrX5LwNnbwyPX7J9MDro8b3aoRVp2bM4FRZPsOZOoFtjVuwt9bNnniWqzIaCfTzKNUqdp43ItSk7oFLdqf05gVhM5UtaLZa2BN7KNdb7hSxYu6FQJkt8haSQK3swUaC9qlRFLIPMerbjxxIF8UnuQ7Oe54uyiN7JJaaHErI0m7qo7ir2bkxHvC0aWw9UkT9Z8RKfWeaQBIizqZnicqmXgsekLvqwkZ2jJrLUw180aFr5g7rCEsRtVtRE
    -- Non-admin API key for the L1 with reduced permissions
    -- Used by CDA user-management tests that verify 403 handling
    insert into cwms_20.at_api_keys (userid, key_name, apikey, created, expires) values ('L1HECTEST', 'non_admin_test_key', 'ak1_SZUNxN3n$argon2id$v=19$m=19456,t=2,p=1$KAlZGdgEboEHvEVcpNGD0g$pPCCkQfOx8v5HNpwPadJNUHGI0I40a7HgcZ7JsTE6T0', sysdate, sysdate + 365);
    -- key is  ak1_SZUNxN3nDx0NpUfOJxpOuGqbqRKdYjbx86x6YVISLb9DiBi3Io5o6T6UFvkHknjIRnRO6oQfA1q6rP4XRDYMH9Hlr4ndffL6NjxPUaBZLSnqukV0uGuZKOUWBB04L5SyloJniOHkFe6ymvB9tzeziGYwzrDv3k6lzacG9vftHkCHB1QbjwwCC0sDkFvuwCe9qnyx5us11qL0YAfKXhe0fBCA2TmNDz8WXfw1HfBnAKx6WD7KqHngplWu4miOvkNverxFmAdJ
end;
/
quit;

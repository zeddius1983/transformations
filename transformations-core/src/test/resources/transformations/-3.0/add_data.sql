--<transformation>
--<update>
INSERT INTO User (email, password, fullname, isAdmin) VALUES ("johnb@my.com", "****", "John B Good", true);
--</update>

--<rollback>
DELETE FROM User WHERE id = 1;
--</rollback>

--</transformation>
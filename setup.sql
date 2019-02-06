CREATE TABLE Customer (
    uid int PRIMARY KEY,
    fullName varchar(100),
    handle varchar(50) UNIQUE NOT NULL,
    pssword varchar(50) UNIQUE NOT NULL
)

CREATE TABLE Reservation (
    uid int REFERENCES Customer(uid),
    fid int REFERENCES Flights(fid),
    PRIMARY KEY(uid, fid)
);

INSERT INTO Customer VALUES (1, 'Fan Yang', 'fyang', '123ab');
INSERT INTO Customer VALUES (2, 'Fan Yang', 'fyang1', '1');
INSERT INTO Customer VALUES (3, 'Fan Yang', 'fyang2', '2');

INSERT Into Reservation VALUES (1, 1491);
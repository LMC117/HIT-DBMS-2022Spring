CREATE TABLE Student (
Sno CHAR(6) PRIMARY KEY,
Sname VARCHAR(10) NOT NULL,
Ssex CHAR CHECK (Ssex IN ('M', 'F')),
Sage INT CHECK (Sage > 0),
Sdept VARCHAR(20)
);

INSERT INTO Student
VALUES
('PH-001', 'Nick', 'M', 20, 'Physics'),
('CS-001', 'Elsa', 'F', 19, 'CS'),
('CS-002', 'Ed', 'M', 19, 'CS'),
('MA-001', 'Abby', 'F', 18, 'Math'),
('MA-002', 'Cindy', 'F', 19, 'Math')
;


CREATE TABLE Course (Cno CHAR(4) PRIMARY KEY);

INSERT INTO Course
VALUES
('1002'),
('2003'),
('3006')
;

CREATE TABLE SC (
Sno CHAR(6),
Cno CHAR(4),
Grade INT,
PRIMARY KEY (Sno, Cno),
FOREIGN KEY (Sno) REFERENCES Student(Sno),
FOREIGN KEY (Cno) REFERENCES Course(Cno)
);

INSERT INTO SC
VALUES
('PH-001', '1002', 92),
('PH-001', '2003', 85),
('PH-001', '3006', 88),
('CS-001', '1002', 95),
('CS-001', '3006', 90),
('CS-002', '3006', 80),
('MA-001', '1002', NULL)
;

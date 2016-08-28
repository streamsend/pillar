-- description: modify and migrate
-- authoredAt: 1370028265000
-- up:

-- stage: 1
create table person (
  name text,
  surname text,
  age int,
  primary key(age, name, surname)
)

-- stage: 2
insert into person (name, surname, age) values ('mustafa', 'gunes', 24)

-- stage: 3
insert into person (name, surname, age) values ('ali', 'yildiz', 25)

-- stage: 4
insert into person (name, surname, age) values ('ayse', 'yilmaz', 26)

-- stage: 5
insert into person (name, surname, age) values ('fatma', 'gun', 27)

-- stage: 6
insert into person (name, surname, age) values ('celebi', 'murat', 28)

-- stage: 7
create table customer(
  name text,
  age int,
  point int,
  primary key(name)
)

-- stage: 8
insert into customer (name, age, point) values ('fatma', 27, 115)

-- stage: 9
insert into customer (name, age, point) values ('ayse', 26, 156)

-- stage: 10
insert into customer (name, age, point) values ('ali', 25, 108)

-- stage: 11
insert into customer (name, age, point) values ('mustafa', 24, 111)

-- stage: 12
create table test_person (
  name text,
  surname text,
  point int,
  city text,
  primary key(name)
)

-- mapping:
-- fetch: eager

-- table: person->test_person
name->/home/mustafa/githubProjects/pillar/src/test/resources/pillar/test_person_name.sh $name test
point->select point from customer where name = '$name'
-- end:




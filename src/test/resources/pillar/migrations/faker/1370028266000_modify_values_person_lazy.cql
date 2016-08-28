-- description: modify and migrate with lazy
-- authoredAt: 1370028265000
-- up:

-- stage: 1
create table person_lazy (
  name text,
  surname text,
  age int,
  primary key(age, name, surname)
)

-- stage: 2
insert into person_lazy (name, surname, age) values ('mustafa', 'gunes', 24)

-- stage: 3
insert into person_lazy (name, surname, age) values ('ali', 'yildiz', 25)

-- stage: 4
insert into person_lazy (name, surname, age) values ('ayse', 'yilmaz', 26)

-- stage: 5
insert into person_lazy (name, surname, age) values ('fatma', 'gun', 27)

-- stage: 6
insert into person_lazy (name, surname, age) values ('celebi', 'murat', 28)

-- stage: 7
create table test_person_lazy (
  name text,
  surname text,
  primary key(name)
)

-- mapping:
-- fetch: lazy

-- table: person_lazy->test_person_lazy
surname->select surname from person_lazy where name = '$name'
name->/home/mustafa/githubProjects/pillar/src/test/resources/pillar/test_person_name.sh $name test
-- end:




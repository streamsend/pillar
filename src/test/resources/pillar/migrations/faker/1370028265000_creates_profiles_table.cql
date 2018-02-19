-- description: creates profiles table and populate
-- authoredAt: 1370028265000
-- up:

CREATE TABLE profiles (
  email text,
  name text,
  address text,
  PRIMARY KEY (email)
)

INSERT INTO profiles(email, name, address) VALUES(
  'rich@yopmail.com',
  'Rich Halle',
  'Schadowstrasse 124; Dusseldorf')

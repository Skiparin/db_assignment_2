# db_assignment_2

To run this project you need to install a few things.

Firstly you need to have docker installed.

Then you need to create a docker container.

docker run --rm -v $(pwd)/data:/data/db --publish=27017:27017 --name dbms -d mongo

This will give you a key. Use this key to run: docker exec -it 88385afa bash

Then update apt-get: apt-get update

Then install wget and unzip: apt-get install -y wget, unzip

Then download the zip file: wget http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip

Then unconpress the zip file: unzip trainingandtestdata.zip

Then add keys to the csv file: sed -i '1s;^;polarity,id,date,query,user,text\n;' training.1600000.processed.noemoticon.csv

You also need to have python and pymongo installed, this can easily be installed with pip.

Install pip:
sudo apt-get install python-pip python-dev build-essential

Install pymongo:
python -m pip install pymongo

After this, you can download the repo and run the python file.

Results:

I had 1287 errors, when loading the csv into mongodb. 

How many Twitter users are in the database?
659178

Which Twitter users link the most to other Twitter users? (Provide the top ten.)
[{'_id': 'lost_dog', 'text': 549}, {'_id': 'tweetpet', 'text': 310}, {'_id': 'VioletsCRUK', 'text': 251}, {'_id': 'what_bugs_u', 'text': 246}, {'_id': 'tsarnick', 'text': 245}, {'_id': 'SallytheShizzle', 'text': 229}, {'_id': 'mcraddictal', 'text': 217}, {'_id': 'Karen230683', 'text': 216}, {'_id': 'keza34', 'text': 211}, {'_id': 'DarkPiano', 'text': 202}]

Who is are the most mentioned Twitter users? (Provide the top five.)
[{'_id': '@mileycyrus', 'count': 4310}, {'_id': '@tommcfly', 'count': 3836}, {'_id': '@ddlovato', 'count': 3349}, {'_id': '@Jonasbrothers', 'count': 1263}, {'_id': '@DavidArchie', 'count': 1221}]

Who are the most active Twitter users (top ten)?
[{'_id': 'lost_dog', 'count': 549}, {'_id': 'webwoke', 'count': 345}, {'_id': 'tweetpet', 'count': 310}, {'_id': 'SallytheShizzle', 'count': 281}, {'_id': 'VioletsCRUK', 'count': 279}, {'_id': 'mcraddictal', 'count': 276}, {'_id': 'tsarnick', 'count': 248}, {'_id': 'what_bugs_u', 'count': 246}, {'_id': 'Karen230683', 'count': 238}, {'_id': 'DarkPiano', 'count': 236}]

Who are the five most grumpy (most negative tweets). (Provide five users for each group)
[{'_id': 'lost_dog', 'count': 549}, {'_id': 'tweetpet', 'count': 310}, {'_id': 'webwoke', 'count': 264}, {'_id': 'wowlew', 'count': 210}, {'_id': 'mcraddictal', 'count': 210}]

Who are the five the most happy (most positive tweets)?
[{'_id': 'what_bugs_u', 'count': 246}, {'_id': 'DarkPiano', 'count': 231}, {'_id': 'VioletsCRUK', 'count': 218}, {'_id': 'tsarnick', 'count': 212}, {'_id': 'keza34', 'count': 211}]

#!/usr/bin/python
# -*- coding: utf-8 -*-
from magpie_mongo import MagpieModel
from keras.models import load_model
from magpie_mongo.utils import load_from_disk, save_to_disk
import gensim
import json

print "Loading pre-trained word to vec model"
word2vecmodel = gensim.models.Word2Vec.load_word2vec_format('./model/GoogleNews-vectors-negative300.bin', binary=True)
print "model_loaded"
labels = []
with open('labels.json') as data_file:
  labels = json.load(data_file)
scaler = load_from_disk('./scalar/scalar')
keras_model = load_model('./saved_models/trained_model.h5')

model = MagpieModel(keras_model=keras_model, word2vec_model=word2vecmodel, scaler=scaler, labels=labels)
#save_to_disk('./scalar/embeddings', model.word2vec_model)
print "predicting..."

res = model.predict_from_text(u'''Theresa May has announced the government will set out its Brexit plans in a formal policy document.
During Prime Minister's Questions, she said she recognised an "appetite" for a White Paper on her "bold" proposals for negotiations with the EU.
A number of Conservative MPs had joined Labour in asking for such a move.
Labour leader Jeremy Corbyn demanded to know when the paper would be published - his party says this must be before MPs vote on getting Brexit under way.Mrs May's announcement comes a day after the Supreme Court ruled that Parliament - not ministers - must decide whether the government can invoke Article 50 of the Lisbon Treaty, triggering the two year process of leaving the EU.
A parliamentary bill to this effect could be introduced as early as Thursday. The prime minister wants to get negotiations under way by the end of March, leaving the government with a tight timetable.
Before Mrs May's announcement, opposition parties and more than half a dozen Conservative MPs including some ex-ministers had called for a White Paper - a government policy document which sets out proposals for future acts of Parliament - on Brexit.
At Prime Minister's Questions, Mrs May said: "I recognise that there is an appetite in this House to see that plan set out in a White Paper. I can confirm to the House that our plan will be set out in a White Paper published in this House."But she added that she regarded the Article 50 debate as "a separate question" from the publication of what she said would be "a bold vision for Britain for the future".
Mr Corbyn pressed the prime minister for a date for the White Paper's publication, which Labour wants to happen before voting on the bill takes place.
Speaking after Prime Minister's Questions, Mrs May's official spokeswoman said it would come out "in due course".
For Labour, shadow Brexit secretary Sir Keir Starmer said: "This U-turn comes just 24 hours after [Brexit Secretary] David Davis seemed to rule out a White Paper, and failed to answer repeated questions from MPs on all sides of the House.
"The prime minister now needs to confirm that this White Paper will be published in time to inform the Article 50 process, and that it will clear up the inconsistencies, gaps and risks outlined in her speech."
Setting out her Brexit plans last week, Mrs May said the UK would leave the European single market and EU customs union, but promised to work to achieve the best free trade deals possible.
Almost all Conservative MPs are expected to back the government in the Article 50 vote and the bill is likely to pass.
Labour says it will not vote against the bill, but will try to amend it, and the Scottish National Party says it has 50 amendments "ready to go" One of the SNP's MPs, Tommy Sheppard, accused the government of a "major U-turn" over the White Paper, saying Mrs May's announcement during Prime Minister's Questions had been a "theatrical stunt to announce a very important public policy".
The Liberal Democrats, who have only nine MPs but more than 100 peers, say they will vote against triggering Article 50 unless there is a guarantee of the public having a vote on the final deal reached between the UK government and the EU.
The party's Alistair Carmichael said: "This White Paper will only be relevant if it is published before the votes on the Article 50 next week, and if it goes into more detail than May's speech."''')
print res [:20]

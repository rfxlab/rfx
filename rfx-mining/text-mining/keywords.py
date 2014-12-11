import tornado.ioloop
import tornado.web
import urllib
import json
import nltk
from nltk import wordpunct_tokenize
from nltk.corpus import stopwords
from HTMLParser import HTMLParser
from textblob import TextBlob

def unique(a):
    """ return the list with duplicate elements removed """
    return list(set(a))

def intersect(a, b):
    """ return the intersection of two lists """
    return list(set(a) & set(b))

def union(a, b):
    """ return the union of two lists """
    return list(set(a) | set(b))

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

#----------------------------------------------------------------------
def _calculate_languages_ratios(text):
    """
    Calculate probability of given text to be written in several languages and
    return a dictionary that looks like {'french': 2, 'spanish': 4, 'english': 0}
    
    @param text: Text whose language want to be detected
    @type text: str
    
    @return: Dictionary with languages and unique stopwords seen in analyzed text
    @rtype: dict
    """

    languages_ratios = {}

    '''
    nltk.wordpunct_tokenize() splits all punctuations into separate tokens
    
    >>> wordpunct_tokenize("That's thirty minutes away. I'll be there in ten.")
    ['That', "'", 's', 'thirty', 'minutes', 'away', '.', 'I', "'", 'll', 'be', 'there', 'in', 'ten', '.']
    '''

    tokens = wordpunct_tokenize(text)
    words = [word.lower() for word in tokens]

    # Compute per language included in nltk number of unique stopwords appearing in analyzed text
    for language in stopwords.fileids():
        stopwords_set = set(stopwords.words(language))
        words_set = set(words)
        common_elements = words_set.intersection(stopwords_set)

        languages_ratios[language] = len(common_elements) # language "score"

    return languages_ratios


#----------------------------------------------------------------------
def detect_language(text):
    """
    Calculate probability of given text to be written in several languages and
    return the highest scored.
    
    It uses a stopwords based approach, counting how many unique stopwords
    are seen in analyzed text.
    
    @param text: Text whose language want to be detected
    @type text: str
    
    @return: Most scored language guessed
    @rtype: str
    """

    ratios = _calculate_languages_ratios(text)

    most_rated_language = max(ratios, key=ratios.get)

    return most_rated_language

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        title = urllib.unquote(self.get_argument('title', default='', strip=True))
        #do not trust the client 
        content = strip_tags(urllib.unquote(self.get_argument('content', default='', strip=True)))
        language = detect_language(content)

        print(language)
        #print(content)
        # just support English for this hackathon vietnam 2014
        if 'english' == language :
            print('en')
            blob_title = TextBlob(title)
            blob_content = TextBlob(content)
            a = blob_title.noun_phrases
            b = blob_content.noun_phrases

            keywords = union(b, a)   
            stopset = set(stopwords.words('english'))  
            
            keywords = [w for w in keywords if not w in stopset]            
    
            #self.write("<br> blob_title " + json.dumps(blob_title.noun_phrases))
            #self.write("<br> blob_content " + json.dumps(blob_content.noun_phrases))
            #self.write("<br> keywords " + json.dumps(keywords))
            self.write(json.dumps(keywords))        

application = tornado.web.Application([
    (r"/keywords", MainHandler),
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
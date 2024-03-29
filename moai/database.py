import datetime
import json
import re
from pkg_resources import iter_entry_points

import sqlalchemy as sql

from moai.utils import check_type

def get_database(uri, config=None):
    prefix = uri.split(':')[0]
    for entry_point in iter_entry_points(group='moai.database', name=prefix):
        dbclass = entry_point.load()
        try:
            return dbclass(uri, config)
        except TypeError:
            # ugly backwards compatibility hack
            return dbclass(uri)
    raise ValueError('No such database registered: %s' % prefix)


class SQLDatabase(object):
    """Sql implementation of a database backend
    This implements the :ref:`IDatabase` interface, look there for
    more documentation.
    """

    table_names = {'language': 'utils_language',
                    'tag': 'utils_tag',
                    'author': 'persons_person',
                    'publication': 'publications_publication',
                    'pub_author': 'publications_publicationauthor',
                    'pub_tag': 'publications_publicationtag',
                    'proceeding': 'publications_proceedings',
                    'paper': 'publications_conferencepaper',
                    'magazine': 'publications_magazine',
                    'mag_article': 'publications_magazinearticle',
                    'journal': 'publications_journal',
                    'jour_article': 'publications_journalarticle',
                    'book': 'publications_book',
                    'section': 'publications_booksection',
                    'thesis': 'publications_thesis',
                    'abstract': 'publications_thesisabstract'
                  }

    #TODO add some real info here.
    sets = json.loads(
      """[
        {"id": "Proceedings", "name": "Set Proceedings",
          "description": "A set related to Proceedings", "hidden": 0},
        {"id": "ConferencePaper", "name": "Set Conference Paper", 
          "description": "A set related to Conference Papers", "hidden": 0},
        {"id": "Magazine", "name": "Set Magazine",
          "description": "A set related to Magazines", "hidden": 0},
        {"id": "MagazineArticle", "name": "Set Magazine Article",
          "description": "A set related to Magazine Articles", "hidden": 0},
        {"id": "Journal", "name": "Set Journal", "description": "A set related to Journals", "hidden": 0},
        {"id": "JournalArticle", "name": "Set Journal Article",
          "description": "A set related to Journal Articles", "hidden": 0},
        {"id": "Book", "name": "Set Book",
          "description": "A set related to Books", "hidden": 0},
        {"id": "BookSection", "name": "Set Book Section",
          "description": "A set related to Book Sections", "hidden": 0},
        {"id": "Thesis", "name": "Set Thesis",
          "description": "A set related to Thesis", "hidden": 0}
      ]""")

    def __init__(self, dburi=None):
        self._uri = dburi
        self._db = self._connect()
        self._languages = self._db.tables[self.table_names.get('language')]
        self._tags = self._db.tables[self.table_names.get('tag')]
        self._authors = self._db.tables[self.table_names.get('author')]
        self._publication = self._db.tables[self.table_names.get('publication')]
        self._publicationauthor = self._db.tables[self.table_names.get('pub_author')]
        self._publicationtag = self._db.tables[self.table_names.get('pub_tag')]
        self._proceedings = self._db.tables[self.table_names.get('proceeding')]
        self._conferencePaper = self._db.tables[self.table_names.get('paper')]
        self._magazine = self._db.tables[self.table_names.get('magazine')]
        self._magazineArticle = self._db.tables[self.table_names.get('mag_article')]
        self._journal = self._db.tables[self.table_names.get('journal')]
        self._journalArticle = self._db.tables[self.table_names.get('jour_article')]
        self._book = self._db.tables[self.table_names.get('book')]
        self._bookSection = self._db.tables[self.table_names.get('section')]
        self._thesis = self._db.tables[self.table_names.get('thesis')]
        self._thesisabstract = self._db.tables[self.table_names.get('abstract')]
        self._djangoLog = self._db.tables['django_admin_log']
        self._djangoContentType = self._db.tables['django_content_type']
        
    def _connect(self):
        dburi = self._uri
        if dburi is None:
            dburi = 'sqlite:///:memory:'
            
        engine = sql.create_engine(dburi)
        db = sql.MetaData(engine)
         
        sql.Table('utils_language', db,
          sql.Column('id', sql.Integer, primary_key=True),
          sql.Column('name', sql.Unicode, nullable=False),
          sql.Column('slug', sql.Unicode, nullable=False),
          sql.Column('language_tag', sql.Unicode))

        sql.Table('utils_tag', db,
          sql.Column('id', sql.Integer, primary_key=True),
          sql.Column('name', sql.Unicode),
          sql.Column('slug', sql.Unicode))

        sql.Table('persons_person', db,
          sql.Column('id', sql.Integer, primary_key=True),
          sql.Column('first_name', sql.Unicode),
          sql.Column('first_surname', sql.Unicode),
          sql.Column('second_surname', sql.Unicode),
          sql.Column('full_name', sql.Unicode),
          sql.Column('biography', sql.Unicode),
          sql.Column('safe_biography', sql.Unicode),
          sql.Column('title', sql.Unicode),
          sql.Column('gender', sql.Unicode),
          sql.Column('personal_website', sql.Unicode),
          sql.Column('email', sql.Unicode),
          sql.Column('phone_number', sql.Unicode),
          sql.Column('phone_extension', sql.Unicode),
          sql.Column('is_active', sql.Unicode),
          sql.Column('slug', sql.Unicode),
          sql.Column('profile_picture', sql.Unicode),
          sql.Column('profile_konami_code_picture', sql.Unicode),
          sql.Column('konami_code_position', sql.Unicode),
          sql.Column('birth_date', sql.Date))

        sql.Table('publications_publication', db,
          sql.Column('id', sql.Integer, primary_key=True),
          sql.Column('title', sql.Unicode, nullable=False),
          sql.Column('slug', sql.Unicode, nullable=False),
          sql.Column('abstract', sql.Unicode),
          sql.Column('doi', sql.Unicode),
          sql.Column('pdf', sql.Unicode),
          sql.Column('language_id', sql.Integer, sql.ForeignKey('utils_language.id'), default=1),
          sql.Column('published', sql.Date),
          sql.Column('year', sql.Integer, nullable=False),
          sql.Column('bibtex', sql.Unicode),
          sql.Column('child_type', sql.Unicode, nullable=False),
          sql.Column('zotero_key', sql.String))
        
        sql.Table('publications_publicationtag', db,
          sql.Column('id',  sql.Integer, primary_key=True),
          sql.Column('tag_id', sql.Integer),
          sql.Column('publication_id', sql.Integer, sql.ForeignKey('publications_publication.id')))

        sql.Table('publications_publicationauthor', db,
          sql.Column('id', sql.Integer, primary_key=True),
          sql.Column('author_id', sql.Integer, sql.ForeignKey('persons_person.id')),
          sql.Column('publication_id', sql.Integer, sql.ForeignKey('publications_publication.id')),
          sql.Column('position', sql.Integer))

        sql.Table('publications_proceedings', db,
          sql.Column('publication_ptr_id', sql.Integer, primary_key=True),
          sql.Column('publisher', sql.Unicode),
          sql.Column('place', sql.Unicode),
          sql.Column('volume', sql.Unicode),
          sql.Column('isbn', sql.Unicode),
          sql.Column('series', sql.Unicode))

        sql.Table('publications_conferencepaper', db,
          sql.Column('publication_ptr_id', sql.Integer, primary_key=True),
          sql.Column('pages', sql.Unicode),
          sql.Column('short_title', sql.Unicode),
          sql.Column('parent_proceedings_id', sql.Integer, sql.ForeignKey('publications_proceedings.publication_ptr_id')),
          sql.Column('presented_at_id', sql.Integer))

        sql.Table('publications_magazine', db,
          sql.Column('publication_ptr_id', sql.Integer, primary_key=True),
          sql.Column('publisher', sql.Unicode),
          sql.Column('place', sql.Unicode),
          sql.Column('volume', sql.Unicode),
          sql.Column('issn', sql.Unicode),
          sql.Column('issue', sql.Unicode))

        sql.Table('publications_magazinearticle', db,
          sql.Column('publication_ptr_id', sql.Integer, primary_key=True),
          sql.Column('pages', sql.Unicode),
          sql.Column('short_title', sql.Unicode),
          sql.Column('parent_magazine_id', sql.Integer, sql.ForeignKey('publications_magazine.publication_ptr_id')))

        sql.Table('publications_journal', db,
          sql.Column('publication_ptr_id', sql.Integer, primary_key=True),
          sql.Column('publisher', sql.Unicode),
          sql.Column('place', sql.Unicode),
          sql.Column('volume', sql.Unicode),
          sql.Column('issn', sql.Unicode),
          sql.Column('issue', sql.Unicode),
          sql.Column('journal_abbreviation', sql.Unicode),
          sql.Column('quartile', sql.Unicode),
          sql.Column('impact_factor', sql.Numeric))

        sql.Table('publications_journalarticle', db,
          sql.Column('publication_ptr_id', sql.Integer, primary_key=True),
          sql.Column('pages', sql.Unicode),
          sql.Column('short_title', sql.Unicode),
          sql.Column('parent_journal_id', sql.Integer, sql.ForeignKey('publications_journal.publication_ptr_id')),
          sql.Column('individually_published', sql.Date))

        sql.Table('publications_book', db,
          sql.Column('publication_ptr_id', sql.Integer, primary_key=True),
          sql.Column('publisher', sql.Unicode),
          sql.Column('place', sql.Unicode),
          sql.Column('volume', sql.Unicode),
          sql.Column('isbn', sql.Unicode),
          sql.Column('edition', sql.Unicode),
          sql.Column('series', sql.Unicode),
          sql.Column('series_number', sql.Integer),
          sql.Column('number_of_pages', sql.Integer),
          sql.Column('number_of_volumes', sql.Integer))

        sql.Table('publications_booksection', db,
          sql.Column('publication_ptr_id', sql.Integer, primary_key=True),
          sql.Column('pages', sql.Unicode),
          sql.Column('short_title', sql.Unicode),
          sql.Column('parent_book_id', sql.Integer, sql.ForeignKey('publications_book.publication_ptr_id')),
          sql.Column('presented_at_id', sql.Integer))

        sql.Table('publications_thesis', db,
          sql.Column('id', sql.Integer, primary_key=True),
          sql.Column('title', sql.Unicode, nullable=False),
          sql.Column('author_id', sql.Integer, sql.ForeignKey('persons_person.id'), nullable=False),
          sql.Column('slug', sql.Unicode, nullable=False),
          sql.Column('advisor_id', sql.Integer, sql.ForeignKey('persons_person.id'), nullable=False),
          sql.Column('registration_date', sql.Date),
          sql.Column('year', sql.Integer, nullable=False),
          sql.Column('main_language_id', sql.Integer, sql.ForeignKey('utils_language.id')),
          sql.Column('pdf', sql.Unicode),
          sql.Column('phd_program_id', sql.Integer),
          sql.Column('number_of_pages', sql.Integer),
          sql.Column('viva_date', sql.DateTime(timezone=True), nullable=False),
          sql.Column('viva_outcome', sql.Unicode, nullable=False),
          sql.Column('held_at_university_id', sql.Integer),
          sql.Column('special_mention', sql.Unicode))

        sql.Table('publications_thesisabstract', db,
          sql.Column('id', sql.Integer, nullable=False),
          sql.Column('thesis_id', sql.Integer, nullable=False),
          sql.Column('language_id', sql.Integer, nullable=False),
          sql.Column('abstract', sql.Integer, nullable=False))

        sql.Table('django_content_type', db,
          sql.Column('id', sql.Integer, primary_key=True),
          sql.Column('name', sql.Unicode, nullable=False),
          sql.Column('app_label', sql.Unicode, nullable=False),
          sql.Column('model', sql.Unicode, nullable=False))

        sql.Table('django_admin_log', db,
          sql.Column('id', sql.Integer, primary_key=True),
          sql.Column('action_time', sql.DateTime(timezone=True), nullable=False),
          sql.Column('user_id', sql.Integer, nullable=False),
          sql.Column('content_type_id', sql.Integer, sql.ForeignKey('django_content_type.id')),
          sql.Column('object_id', sql.Integer),
          sql.Column('object_repr', sql.Unicode, nullable=False),
          sql.Column('action_flag', sql.Unicode, nullable=False),
          sql.Column('change_message', sql.Unicode, nullable=False))

        db.create_all()
        return db

    def get_set(self, oai_id):
        """row = self._sets.select(
            self._sets.c.set_id == oai_id).execute().fetchone()
        if row is None:
            return
        return {'id': row.set_id,
                'name': row.name,
                'description': row.description,
                'hidden': row.hidden}"""
        return

    def get_setrefs(self, oai_id, include_hidden_sets=False):
        """set_ids = []
        query = sql.select([self._setrefs.c.set_id])
        query.append_whereclause(self._setrefs.c.record_id == oai_id)
        if include_hidden_sets == False:
            query.append_whereclause(
                sql.and_(self._sets.c.set_id == self._setrefs.c.set_id,
                         self._sets.c.hidden == include_hidden_sets))
        
        for row in query.execute():
            set_ids.append(row[0])
        set_ids.sort()
        return set_ids"""

    def record_count(self):
        return sql.select([sql.func.count('*')],
                          from_obj=[self._records]).execute().fetchone()[0]

    def set_count(self):
        return sql.select([sql.func.count('*')],
                          from_obj=[self._sets]).execute().fetchone()[0]

    def oai_sets(self, offset=0, batch_size=20):
        index = offset
        while index < offset + batch_size and index < len(self.sets):
          if self.sets[index].get('hidden') == False:
            yield {
                    'id': self.sets[index].get('id'),
                    'name': self.sets[index].get('name'),
                    'description': self.sets[index].get('description')
                  }
          index += 1

    def oai_earliest_datestamp(self):
        earliest_datestamp = datetime.datetime(1970, 1, 1)
        publication_earliest_datestamp = sql.select([self._publication.c.year],
          order_by=[sql.asc(self._publication.c.year)]).limit(1).execute().fetchone()
        thesis_earliest_datestamp = sql.select([self._thesis.c.year],
          order_by=[sql.asc(self._thesis.c.year)]).limit(1).execute().fetchone()
        if publication_earliest_datestamp:
          earliest_datestamp = publication_earliest_datestamp[0]
        if thesis_earliest_datestamp:
          if earliest_datestamp > thesis_earliest_datestamp[0]:
            return datetime.datetime(thesis_earliest_datestamp[0], 1, 1)
        return datetime.datetime(earliest_datestamp, 1, 1)
    
    def process_control_char_word_break(self, text):
      p = re.compile(ur'-\n', re.UNICODE)
      processed_text = re.sub(p, u'', text)
      processed_text = processed_text.replace('\\', '\\\\')
      processed_text = processed_text.replace('\n', ' ')
      processed_text = processed_text.replace('\"', '\\"')
      processed_text = processed_text.replace('\r', '')
      processed_text = processed_text.replace('\t', '')
      processed_text = processed_text.replace('\b', '')
      processed_text = processed_text.replace('\f', '')

      return processed_text

    def search_language(self, lg_id):
      return self._languages.select(
        self._languages.c.id == lg_id).execute().fetchone()

    def search_proceeding(self, pr_id):
      return self._proceedings.select(
        self._proceedings.c.publication_ptr_id == pr_id).execute().fetchone()

    def search_magazine(self, mg_id):
      return self._magazine.select(
        self._magazine.c.publication_ptr_id == mg_id).execute().fetchone()

    def search_journal(self, jr_id):
      return self._journal.select(
        self._journal.c.publication_ptr_id == jr_id).execute().fetchone()

    def search_book(self, bk_id):
      return self._book.select(
        self._book.c.publication_ptr_id == bk_id).execute().fetchone()

    def search_all_records_author(self, record_id):
      return self._publicationauthor.select(
        self._publicationauthor.c.publication_id == record_id).execute().fetchall()

    def search_author(self, author_id):
      return self._authors.select(
        self._authors.c.id == author_id).execute().fetchone()

    def search_all_tags(self, record_id):
      return self._publicationtag.select(
        self._publicationtag.c.publication_id == record_id).execute().fetchall()

    def seach_tag(self, tg_id):
      return self._tags.select(
          self._tags.c.id == tg_id).execute().fetchone()

    def search_all_thesis_abstract(self, thesis_id):
      return self._thesisabstract.select(
        self._thesisabstract.c.thesis_id == thesis_id).execute().fetchall()

    def search_content_type(self, app_label, model):
      return self._djangoContentType.select(
        self._djangoContentType.c.model == model).where(
        self._djangoContentType.c.app_label == app_label).execute().fetchone()

    def search_logs(self, content_type, object_id):
      return self._djangoLog.select().where(
        self._djangoLog.c.object_id == unicode(object_id)).where(
        self._djangoLog.c.content_type_id == content_type).execute().fetchall()

    def get_publication_setspec(self, dc_type):
      setspec = None
      if dc_type == 'Proceedings':
        setspec = self.sets[0]
      elif dc_type == 'ConferencePaper':
        setspec = self.sets[1]
      elif dc_type == 'Magazine':
        setspec = self.sets[2]
      elif dc_type == 'MagazineArticle':
        setspec = self.sets[3]
      elif dc_type == 'Journal':
        setspec = self.sets[4]
      elif dc_type == 'JournalArticle':
        setspec = self.sets[5]
      elif dc_type == 'Book':
        setspec = self.sets[6]
      elif dc_type == 'BookSection':
        setspec = self.sets[7]
      return json.loads('{"id": ["' + setspec.get('id') + '"]}').get('id')

    def get_thesis_setspec(self):
      return json.loads('{"id": ["' + self.sets[8].get('id') + '"]}').get('id')

    def get_parent_type(self, dc_type, record_id):
      child = None
      if dc_type == 'Proceedings':
        proceeding = self.search_proceeding(record_id)
        if proceeding is not None:
          child = proceeding
      elif dc_type == 'ConferencePaper':
        conferencePaper = self._conferencePaper.select(
          self._conferencePaper.c.publication_ptr_id == record_id).execute().fetchone()
        if conferencePaper is not None:
          proceeding = self.search_proceeding(conferencePaper.parent_proceedings_id)
          if proceeding is not None:
            child = proceeding
      elif dc_type == 'Magazine':
        magazine = self.search_magazine(record_id)
        if magazine is not None:
          child = magazine
      elif dc_type == 'MagazineArticle':
        magazineArticle = self._magazineArticle.select(
          self._magazineArticle.c.publication_ptr_id == record_id).execute().fetchone()
        if magazineArticle is not None:
          magazine = self.search_magazine(magazineArticle.parent_magazine_id)
          if magazine is not None:
            child = magazine
      elif dc_type == 'Journal':
        journal = self.search_journal(record_id)
        if journal is not None:
          child = journal
      elif dc_type == 'JournalArticle':
        journalArticle = self._journalArticle.select(
          self._journalArticle.c.publication_ptr_id == record_id).execute().fetchone()
        if journalArticle is not None:
          journal = self.search_journal(journalArticle.parent_journal_id)
          if journal is not None:
            child = journal
      elif dc_type == 'Book':
        book = self.search_book(record_id)
        if book is not None:
          child = book
      elif dc_type == 'BookSection':
        bookSection = self._bookSection.select(
          self._bookSection.c.publication_ptr_id == record_id).execute().fetchone()
        if bookSection is not None:
          book = self.search_book(bookSection.parent_book_id)
          if book is not None:
            child = book
      return child

    def get_publisher(self, dc_type, record_id):
      return self.get_parent_type(dc_type, record_id).publisher

    def get_pubmetadata(self, record):
      metadata = '{"title":["' + self.process_control_char_word_break(record.title) + '"], ' \
        '"date":["' + record.published.strftime('%Y-%m-%dT%H:%M:%SZ') + '"]' \
        ', "format":["digital"], "type":["' + record.child_type + '"]'
      publisher = self.get_publisher(record.child_type, record.id)
      if publisher is not None:
        metadata += ', "publisher":["' + publisher + '"]'
      if record.abstract is not None:
        metadata += ', "description":["' + self.process_control_char_word_break(record.abstract) + '"]'
      if(record.language_id is not None):
        language = self.search_language(record.language_id)
        if language is not None:
          if language.language_tag is not None:
            metadata += ', "language":["' + language.language_tag + '"]'
      else:
        metadata += ', "language":["en"]'
      authors_ref = self.search_all_records_author(record.id)
      for index in range(len(authors_ref)):
        author = self.search_author(authors_ref[index].author_id)
        if author is not None:
            if index == 0:
              metadata += ', "creator":["' + author.full_name + '"'
              if len(authors_ref) == 1:
                metadata += ']'
            elif index == len(authors_ref) - 1:
              metadata += ', "' + author.full_name + '"]'
            else:
              metadata += ', "' + author.full_name + '"'
      tags_ref = self.search_all_tags(record.id)
      for index in range(len(tags_ref)):
        tag = self.seach_tag(tags_ref[index].tag_id)
        if tag is not None:
          if index == 0:
             metadata += ', "subject":["' + tag.name + '"'
             if len(tags_ref) == 1:
              metadata += ']'
          elif index == len(tags_ref) - 1:
            metadata += ', "' + tag.name + '"]'
          else:
            metadata += ', "' + tag.name + '"'
      return metadata + '}'

    def get_thmetadata(self, record):
      metadata = '{"type":["doctoral dissertation"], "format":["digital"]'
      metadata += ', "title":["' + self.process_control_char_word_break(record.title) + '"]'
      if record.registration_date is not None:
        metadata += ', "date":["' + record.registration_date.strftime('%Y-%m-%dT%H:%M:%SZ') + '"]'
      author = self.search_author(record.id)
      if author is not None:
        metadata += ', "creator": ["' + author.full_name + '"]'
      row = self._languages.select(self._languages.c.id == record.main_language_id).execute().fetchone()
      if row is not None:
        if row.language_tag is not None:
          metadata += ', "language":["' + row.language_tag + '"]'
      descriptions = self.search_all_thesis_abstract(record.id)
      if descriptions is not None:
        for index in range(len(descriptions)):
          if index == 0:
            metadata += ', "description":["' + self.process_control_char_word_break(descriptions[index].abstract) + '"'
          else:
            metadata += ', "' + self.process_control_char_word_break(descriptions[index].abstract) + '"'
        metadata += ']'
      return metadata + '}'

    def get_app_label(self, tablename):
      return tablename.split('_')[0]

    def get_model_id(self, tablename):
      return tablename.split('_')[1]

    def get_pubmodified_date(self, record):
      #tables to check: publication, language, tag, collectionpublication, person
      
      ids = [[] for i in xrange(5)]
      ids[0].append(record.id)
      dc_type_id = None
      dc_type = self.get_parent_type(record.child_type, record.id)
      if dc_type is not None:
        ids[1].append(dc_type.publication_ptr_id)
      ids[2].append(record.language_id)
      if ids[2][0] is None:
        ids[2][0] = 1
      authors = self.search_all_records_author(record.id)
      for author in authors:
        ids[3].append(author.author_id)
      tags = self.search_all_tags(record.id)
      for tag in tags:
        ids[4].append(tag.tag_id)
      
      tables = []
      tables.append(self.table_names.get('publication'))
      tables.append('publications_' + record.child_type.lower())
      tables.append(self.table_names.get('language'))
      tables.append(self.table_names.get('author'))
      tables.append(self.table_names.get('tag'))

      logs_query = sql.select([self._djangoLog.c.action_time],
        from_obj=[self._djangoLog]).order_by(sql.desc(self._djangoLog.c.action_time))
      where_clauses = []

      for index in range(len(tables)):
        content_type = self.search_content_type(self.get_app_label(tables[index]), self.get_model_id(tables[index]))
        if content_type is not None:
          for j_index in range(len(ids[index])):
            where_clauses.append(sql.and_(
              self._djangoLog.c.content_type_id == content_type.id, self._djangoLog.c.object_id == unicode(ids[index][j_index])))
      logs_query.append_whereclause(sql.or_(*where_clauses))
      row = logs_query.execute().fetchone()
      if row is not None:
        return row[0].replace(tzinfo=None)
      else:
        return datetime.datetime(record.year, 1, 1)

    def get_thmodified_date(self, record):
      #tables to check: thesis, language, person, abstract
      
      ids = [[] for i in xrange(4)]
      ids[0].append(record.id)
      ids[1].append(record.main_language_id)
      ids[2].append(record.author_id)
      abstracts = self.search_all_thesis_abstract(record.id)
      for abstract in abstracts:
        ids[3].append(abstract.id)

      tables = []
      tables.append(self.table_names.get('thesis'))
      tables.append(self.table_names.get('language'))
      tables.append(self.table_names.get('author'))
      tables.append(self.table_names.get('abstract'))

      logs_query = sql.select([self._djangoLog.c.action_time],
        from_obj=[self._djangoLog]).order_by(sql.desc(self._djangoLog.c.action_time))
      where_clauses = []

      for index in range(len(tables)):
        content_type = self.search_content_type(self.get_app_label(tables[index]), self.get_model_id(tables[index]))
        if content_type is not None:
          for j_index in range(len(ids[index])):
            where_clauses.append(sql.and_(
              self._djangoLog.c.content_type_id == content_type.id, self._djangoLog.c.object_id == unicode(ids[index][j_index])))
      logs_query.append_whereclause(sql.or_(*where_clauses))
      row = logs_query.execute().fetchone()
      if row is not None:
        return row[0].replace(tzinfo=None)
      else:
        return datetime.datetime(record.year, 1, 1)

    def generate_json(self, r_id, delete, modified_timestamp, metadata, sets):
      return {  
                'id': r_id,
                'deleted': delete,
                'modified': modified_timestamp,
                'metadata': metadata,
                'sets': sets
              }

    def oai_query(self,
                  offset=0,
                  batch_size=20,
                  needed_sets=None,
                  disallowed_sets=None,
                  allowed_sets=None,
                  from_date=None,
                  until_date=None,
                  identifier=None):

        """needed_sets = needed_sets or []
        disallowed_sets = disallowed_sets or []
        allowed_sets = allowed_sets or []
        if batch_size < 0:
            batch_size = 0

        query = self._records.select(
            order_by=[sql.desc(self._records.c.modified)])

        # filter sets

        setclauses = []
        for set_id in needed_sets:
            alias = self._setrefs.alias()
            setclauses.append(
                sql.and_(
                alias.c.set_id == set_id,
                alias.c.record_id == self._records.c.record_id))
            
        if setclauses:
            query.append_whereclause((sql.and_(*setclauses)))
            
        allowed_setclauses = []
        for set_id in allowed_sets:
            alias = self._setrefs.alias()
            allowed_setclauses.append(
                sql.and_(
                alias.c.set_id == set_id,
                alias.c.record_id == self._records.c.record_id))
            
        if allowed_setclauses:
            query.append_whereclause(sql.or_(*allowed_setclauses))

        disallowed_setclauses = []
        for set_id in disallowed_sets:
            alias = self._setrefs.alias()
            disallowed_setclauses.append(
                sql.exists([self._records.c.record_id],
                           sql.and_(
                alias.c.set_id == set_id,
                alias.c.record_id == self._records.c.record_id)))
            
        if disallowed_setclauses:
            query.append_whereclause(sql.not_(sql.or_(*disallowed_setclauses)))
            
        for row in query.distinct().offset(offset).limit(batch_size).execute():
            yield {'id': row.record_id,
                   'deleted': row.deleted,
                   'modified': row.modified,
                   'metadata': json.loads(row.metadata),
                   'sets': self.get_setrefs(row.record_id)
                   }"""

        publicationQuery = self._publication.select()
        thesisQuery = self._thesis.select()
        record_id = None
        slug = None
        search_thesis = True
        
        if identifier is not None:
          try:
            splitter = identifier.index('/')
            record_id = int(identifier[:splitter])
            slug = identifier[splitter + 1:]
          except ValueError:
            pass
          publicationQuery.append_whereclause(self._publication.c.id == record_id)
          thesisQuery.append_whereclause(self._thesis.c.id == record_id)

        if until_date == None or until_date > datetime.datetime.utcnow():
          until_date = datetime.datetime.utcnow()

        if needed_sets:
          publicationQuery.append_whereclause(self._publication.c.child_type.in_(needed_sets))
          if self.sets[8].get('id') not in needed_sets:
            search_thesis = False

        totalrows = 0        
        pub_records = 0
        publications = []

        for row in publicationQuery.distinct().order_by(sql.asc(self._publication.c.id)).execute().fetchall():
          modified_timestamp = self.get_pubmodified_date(row)
          if from_date is not None:
            if modified_timestamp >= from_date and modified_timestamp <= until_date:
                totalrows += 1
                publications.append({'record': row, 'modified': modified_timestamp})
          elif modified_timestamp <= until_date:
                totalrows += 1
                publications.append({'record': row, 'modified': modified_timestamp})

        if offset < totalrows:
          th_offset = 0
          index = offset
          while index < len(publications) and pub_records < batch_size:
            yield self.generate_json(str(publications[index].get('record').id)
              + '/' + publications[index].get('record').slug, False,
              publications[index].get('modified'),
              json.loads(self.get_pubmetadata(publications[index].get('record'))), 
              self.get_publication_setspec(publications[index].get('record').child_type))
            index += 1
            pub_records += 1
        else:
          th_offset = offset - totalrows
        if not search_thesis:
          th_limit = 0
        else:
          th_limit = batch_size - pub_records
        
        #TODO change the zero default offset for real value.
        for row in thesisQuery.distinct().offset(th_offset).limit(th_limit).execute():
          modified_timestamp = self.get_thmodified_date(row)
          oai_set = self.get_thesis_setspec()
          if from_date is not None:
            if modified_timestamp >= from_date and modified_timestamp <= until_date:
              yield self.generate_json(str(row.id) + '/' + row.slug, False,
                self.get_thmodified_date(row), json.loads(self.get_thmetadata(row)), oai_set)
          elif modified_timestamp <= until_date:
            yield self.generate_json(str(row.id) + '/' + row.slug, False,
              self.get_thmodified_date(row), json.loads(self.get_thmetadata(row)), oai_set)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Numeric, String, Date, DateTime, Boolean, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
import codecs

Base = declarative_base()

class Language(Base):
	__tablename__ = 'utils_language'

	id = Column(Integer, primary_key=True)
	name = Column(String, nullable=False)
	slug = Column(String, nullable=False)

class Person(Base):
	__tablename__ = 'persons_person'

	id = Column(Integer, primary_key=True)
	first_name = Column(String)
	first_surname = Column(String)
	second_surname = Column(String)
	full_name = Column(String)
	biography = Column(String)
	safe_biography = Column(String)
	title = Column(String)
	gender = Column(String)
	personal_website = Column(String)
	email = Column(String)
	phone_number = Column(String)
	phone_extension = Column(String)
	is_active = Column(Boolean)
	slug = Column(String)
	profile_picture = Column(String)
	profile_konami_code_picture = Column(String)
	konami_code_position = Column(String)
	birth_date = Column(Date)

	def __repr__(self):
		return "<Person>fist_name='%s', first_surname='%s', second_surname='%s', is_active='%s', birth_date='%s'<\Person>" % \
		(self.first_name, self.first_surname, self.second_surname, self.is_active, self.birth_date)

class Book(Base):
	__tablename__ = 'publications_book'

	publication_ptr_id = Column(Integer, primary_key=True)
	publisher = Column(String)
	place = Column(String)
	volume = Column(String)
	isbn = Column(String)
	edition = Column(String)
	series = Column(String)
	series_number = Column(Integer)
	number_of_pages = Column(Integer)
	number_of_volumes = Column(Integer)

	def __repr__(self):
		return "<Book>isbn=%s, edition=%s publisher=%s</Book>" % \
		(self.isbn, self.edition, self.publisher)

class BookSection(Base):
	__tablename__ = 'publications_booksection'

	publication_ptr_id = Column(Integer, primary_key=True)
	pages = Column(String)
	short_title = Column(String)
	parent_book_id = Column(Integer, ForeignKey('publications_book.publication_ptr_id'))
	parent_book = relationship('Book')
	presented_at_id = Column(Integer)

	def __repr__(self):
		return "<BookSection>short_title=%s, parent_book_isbn=%s</BookSection>" % \
		(self.short_title, self.parent_book.isbn)

class Coadvisor(Base):
	__tablename__ = 'publications_coadvisor'

	id = Column(Integer, primary_key=True)
	thesis_id = Column(Integer, ForeignKey('publications_thesis.id'))
	thesis = relationship('Thesis')
	co_advisor_id = Column(Integer, ForeignKey('persons_person.id'))
	co_advisor = relationship('Person')

	def __repr__(self):
		return "<Co-advisor>\n\t%s\n\t%s\n</Co-advisor>" % \
		(self.thesis, self.co_advisor)

class ConferencePaper(Base):
	__tablename__ = 'publications_conferencepaper'

	publication_ptr_id = Column(Integer, primary_key=True)
	pages = Column(String)
	short_title = Column(String)
	parent_proceedings_id = Column(Integer, ForeignKey('publications_proceedings.publication_ptr_id'))
	parent_proceedings = relationship('Proceedings')
	presented_at_id = Column(Integer)

	def __repr__(self):
		return "<ConferencePaper>short_title=%s, parent_proceedings_isbn=%s</ConferencePaper>" % \
		(self.short_title, self.parent_proceedings.isbn)

class Journal(Base):
	__tablename__ = 'publications_journal'

	publication_ptr_id = Column(Integer, primary_key=True)
	publisher = Column(String)
	place = Column(String)
	volume = Column(String)
	issn = Column(String)
	issue = Column(String)
	journal_abbreviation = Column(String)
	quartile = Column(String)
	impact_factor = Column(Numeric)

	def __repr__(self):
		return "<Journal>issn=%s, volume=%s, publisher=%s</Journal>" % \
		(self.issn, self.volume, self.publisher)

class JournalArticle(Base):
	__tablename__ = 'publications_journalarticle'

	publication_ptr_id = Column(Integer, primary_key=True)
	pages = Column(String)
	short_title = Column(String)
	parent_journal_id = Column(Integer, ForeignKey('publications_journal.publication_ptr_id'))
	parent_journal = relationship('Journal')
	individually_published = Column(Date)

	def __repr__(self):
		return "<JournalArticle>short_title=%s, parent_journal_issn=%s, individually_published=%s</JournalArticle>" % \
		(self.short_title, self.parent_journal.issn, self.individually_published)

class Magazine(Base):
	__tablename__ = 'publications_magazine'

	publication_ptr_id = Column(Integer, primary_key=True)
	publisher = Column(String)
	place = Column(String)
	volume = Column(String)
	issn = Column(String)
	issue = Column(String)

	def __repr__(self):
		return "<Magazine>issn=%s, volume=%s, publisher=%s</Magazine>" % \
		(self.issn, self.volume, self.publisher)

class MagazineArticle(Base):
	__tablename__ = 'publications_magazinearticle'

	publication_ptr_id = Column(Integer, primary_key=True)
	pages = Column(String)
	short_title = Column(String)
	parent_magazine_id = Column(Integer, ForeignKey('publications_magazine.publication_ptr_id'))
	parent_magazine = relationship('Magazine')

	def __repr__(self):
		return "<MagazineArticle>short_title=%s, parent_magazine_issn=%s</MagazineArticle>" % \
		(self.short_title, self.parent_magazine.issn)

class Proceedings(Base):
	__tablename__ = 'publications_proceedings'

	publication_ptr_id = Column(Integer, primary_key=True)
	publisher = Column(String)
	place = Column(String)
	volume = Column(String)
	isbn = Column(String)
	series = Column(String)

	def __repr__(self):
		return "<Proceedings>isbn=%s, volume=%s, series=%s</Proceedings>" % \
		(self.isbn, self.volume, self.series)

class Publication(Base):
	__tablename__ = 'publications_publication'

	id = Column(Integer, primary_key=True)
	title = Column(String, nullable=False)
	slug = Column(String, nullable=False)
	abstract = Column(String)
	doi = Column(String)
	pdf = Column(String)
	language_id = Column(Integer, ForeignKey('utils_language.id'))
	published = Column(Date)
	year = Column(Integer, nullable=False)
	bibtex = Column(String)
	child_type = Column(String, nullable=False)
	zotero_key = Column(String)

	def __repr__(self):
		return "<Publication>title='%s', slug='%s', language='%s', published='%s'</Publication>" % \
		(self.title, self.slug, self.language_id, self.published)

class PublicationAuthor(Base):
	__tablename__ = 'publications_publicationauthor'

	id = Column(Integer, primary_key=True)
	author_id = Column(Integer, ForeignKey('persons_person.id'))
	author = relationship('Person')
	publication_id = Column(Integer, ForeignKey('publications_publication.id'))
	publication = relationship('Publication')
	position = Column(Integer)

	def __repr__(self):
		return "<Publication-Autor>\n\t%s\n\t%s\n</Publication-Autor>" % \
		(self.author, self.publication)

class PublicationEditor(Base):
	__tablename__ = 'publications_publicationeditor'

	id = Column(Integer, primary_key=True)
	editor_id = Column(Integer, ForeignKey('persons_person.id'))
	editor = relationship('Person')
	publication_id = Column(Integer, ForeignKey('publications_publication.id'))
	publication = relationship('Publication')

	def __repr__(self):
		return "<Publication-Editor>\n\t%s\n\t%s\n</Publication-Editor>" % \
		(self.editor, self.publication)

class PublicationRank(Base):
	__tablename__ = 'publications_publicationrank'

	id = Column(Integer, primary_key=True)
	publication_id = Column(Integer, ForeignKey('publications_publication.id'))
	publication = relationship('Publication')
	ranking_id = Column(Integer, ForeignKey('publications_ranking.id'))
	ranking = relationship('Ranking')

	def __repr__(self):
		return "<Publication-Rank>\n\t%s\n\t%s\n</Publication-Rank>" % \
		(self.publication, self.ranking)

class PublicationSeeAlso(Base):
	__tablename__ = 'publications_publicationseealso'

	id = Column(Integer, primary_key=True)
	publication_id = Column(Integer, ForeignKey('publications_publication.id'))
	publication = relationship('Publication')
	see_also = Column(String)

	def __repr__(self):
		return "<Publication-SeeAlso>\n\t%s\n\tsee_also=%s\n</Publication-SeeAlso>" % \
		(self.publication, self.see_also)

class PublicationTag(Base):
	__tablename__ = 'publications_publicationtag'

	id = Column(Integer, primary_key=True)
	tag_id = Column(Integer)
	publication_id = Column(Integer, ForeignKey('publications_publication.id'))
	publication = relationship('Publication')

	def __repr__(self):
		return "<Publication-Tag>\n\t%s\n\ttag_id=%s\n</Publication-Tag>" % \
		(self.publication, self.tag_id)

class Ranking(Base):
	__tablename__ = 'publications_ranking'

	id = Column(Integer, primary_key=True)
	name = Column(String)
	icon = 	Column(String)
	slug = Column(String, nullable=False)

	def __repr__(self):
		return "<Ranking>name=%s, slug=%s</Ranking>" % \
		(self.name, self.slug)

class Thesis(Base):
	__tablename__ = 'publications_thesis'

	id = Column(Integer, primary_key=True)
	title = Column(String, nullable=False)
	author_id = Column(Integer, ForeignKey('persons_person.id'), nullable=False)
	author = relationship('Person', foreign_keys=[author_id])
	slug = Column(String, nullable=False)
	advisor_id = Column(Integer, ForeignKey('persons_person.id'), nullable=False)
	advisor = relationship('Person', foreign_keys=[advisor_id])
	registration_date = Column(Date)
	year = Column(Integer, nullable=False)
	main_language_id = Column(Integer, ForeignKey('utils_language.id'))
	main_language = relationship('Language')
	pdf = Column(String)
	phd_program_id = Column(Integer)
	number_of_pages = Column(Integer)
	viva_date = Column(DateTime(timezone=True), nullable=False)
	viva_outcome = Column(String, nullable=False)
	held_at_university_id = Column(Integer)
	special_mention = Column(String)

	def __repr__(self):
		return "<Thesis>title=%s, author=%s, year=%s, advidor=%s</Thesis>" % \
		(self.title, self.author.full_name, self.year, self.advisor.full_name)

dburi = 'postgresql+psycopg2://meister:1234@/labman?client_encoding=utf8'

engine = create_engine(dburi)

Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

for row in session.query(PublicationTag).all():
	print unicode(row)

Mass Scraping
=============

Mass Scraping is a module to quickly download and scrape websites on a massive scale. It has been successfully used to download and scrape web resources such as [PubMed](http://www.ncbi.nlm.nih.gov/pubmed) (20M documents) or [IMDb](http://www.imdb.com/) (1.2M documents). This module was first created to scrape information from the California's state licensing board of contractors in order to build [Chiefmall.com](http://www.chiefmall.com).

The use of this module goes through four steps. First you generate a list of URLs. Second you use retrieve.py to massively download from the list of URLs. The raw data is stored and possibly compressed in a efficient directory structure called a repository. Third a program called extract.py is used to parse the information of interest from the files in a repository using configuration files. Configuration files are made of regular expressions, post transform callback functions and optionaly SQL type fields for populate.py program (see below for explanation). Fourth populate.py is used to populate the information into the database.

A toy example illustrating all steps is provided in the example/ directory. The process is applied in order to scrape all movie information from IMDb.

1. Generate URLs
----------------

The first thing you need to do is look for patterns in the URLs. For example in IMDb, URLs are generated as www.imdb.com/title/tt{title_id}. A scripts could be written to list all the URLs of interest. In the example directory we only have 10 URLs from IMDb in example/urls/urls.

2. Retrieve
-----------

Next you use the program retrieve.py to masively download all the data from your list URLs. With retrieve you can control the number of parallel threads, sleep after x number of seconds or shuffe the list of urls. The data may be stored in an efficient directory structure called a repository. 

A repository is simply an MD5 named file directory structure of a chosen depth and with possible compression at its leaves. For example the URL  http://www.imdb.com/title/tt0110912/ is stored in the file 9d45e6808a9d0c7a44406942a6ff3b41 which dependng on options may be accessed through ./9d/45.zip/9d45e6808a9d0c7a44406942a6ff3b41.

For the sake of our IMDb example we run:

	python retrieve.py -o example/data/ example/urls/urls > example/urls/urls.retrieved

	example/data : flat repository without comnpression.
	example/urls/urls : list of URLs to download.

3. Extract
----------

Next you need to create configuration files for extract.py. A configuration file is composed of a list of fields starting with the symbol "@". Each field has a regular expression and a possible post processing callback function. Additionally each field could have an SQL type statement to specify how the results will be populated when using populate.py (see below). 

For example when scraping IMDb the following configuration file could be made to get the id and title of each movie:

	regex_mode = 'inline'
	regex_flag = re.I|re.S

	@imdb_id =  dict(
		regex = 'href="http://www.imdb.com/title/tt(\d+)/"',
		sql   = 'int(12) unsigned primary key'
	)

	@title = dict(
		regex = '<h1>(.+?)<span>\(<a href="/Sections/Years/\d+/">', 
		callback = lambda s: strips(s, '"'),
		sql   = 'varchar(250)'
	)

The regex_mode could be either "inline" or "global". In inline mode extract.py only gets the first matching text, whereas in global mode it gets them all. The callback function for the "title" field tells extract to strip the quotes from the beginning and the end of the resulting matching text. Note that a configuation file is plain python code with the additional "@" that marks each field.

For the sake of our IMDb example we run:

	python extract.py -c 'example/conf/*' -e 'iso-8859-1' -o example/tables/ example/data/

	example/conf/ : the configuration files for extract.py.
	example/tables/ : where to store the plain text tables.

4. Populate
-----------

The program extract.py puts the results into a plain text table which then could be populated to the database using populate.py. The program populate.py takes these plain text tables together with the configuration files and populate each field them into a database.

For the sake of our IMDb example we run:

	python populate.py -d example/conf/titles.conf example/tables/titles.tbl titles
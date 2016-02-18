# -*- coding: utf-8 -*-


# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CourtlistenerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    link = scrapy.Field()
    dateFiled = scrapy.Field()
    statu = scrapy.Field()
    dockeno = scrapy.Field()
    court = scrapy.Field()
    citations = scrapy.Field()
    opinionid =  scrapy.Field()
    judges = scrapy.Field()


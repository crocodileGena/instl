#!/usr/bin/env python3


import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import update
from sqlalchemy import or_
from sqlalchemy.ext import baked
from sqlalchemy import bindparam
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import func
from sqlalchemy import event
from sqlalchemy import text
from sqlalchemy.engine import reflection

import pyinstl
from pyinstl import itemRow
from pyinstl.itemRow import ItemRow, ItemDetailRow, ItemToDetailRelation, alchemy_base

import utils
from configVar import var_stack
from functools import reduce
from aYaml import YamlReader


class ItemTableYamlReader(YamlReader):
    def __init__(self):
        super().__init__()
        self.items, self.details = list(), list()

    def init_specific_doc_readers(self): # this function must be overridden
        self.specific_doc_readers["!index"] = self.read_index_from_yaml

    def read_index_from_yaml(self,all_items_node):
        for IID in all_items_node:
            item, item_details = ItemTableYamlReader.item_dicts_from_node(IID, all_items_node[IID])
            self.items.append(item)
            self.details.extend(item_details)

    @staticmethod
    def item_dicts_from_node(the_iid, the_node):
        item, details = dict(), list()
        item['iid'] = the_iid
        item['inherit_resolved'] = False
        details = ItemTableYamlReader.read_item_details_from_node(the_iid, the_node)
        return item, details

    @staticmethod
    def read_item_details_from_node(the_iid, the_node, the_os='common'):
        details = list()
        for detail_name in the_node:
            if detail_name in ItemTable.os_names[1:]:
                os_specific_details = ItemTableYamlReader.read_item_details_from_node(the_iid, the_node[detail_name], detail_name)
                details.extend(os_specific_details)
            elif detail_name == 'actions':
                actions_details = ItemTableYamlReader.read_item_details_from_node(the_iid, the_node['actions'], the_os)
                details.extend(actions_details)
            else:
                for details_line in the_node[detail_name]:
                    details.append({'iid': the_iid, 'os': the_os, 'detail_name': detail_name, 'detail_value': details_line.value, 'inherited': False})
        return details

class ItemTable(object):
    os_names = ('common', 'Mac', 'Mac32', 'Mac64', 'Win', 'Win32', 'Win64')
    dont_inherit_details = ('name','inherit')
    def __init__(self):
        self.engine = create_engine('sqlite:///:memory:', echo=False)
        alchemy_base.metadata.create_all(self.engine)
        self.session_maker = sessionmaker(bind=self.engine)
        self.session = self.session_maker()
        self.baked_queries_map = self.bake_baked_queries()
        self.bakery = baked.bakery()
        self._get_for_os = [ItemTable.os_names[0]]

    def begin_get_for_all_oses(self):
        """ adds all known os names to the list of os that will influence all get functions
            such as depend_list, source_list etc.
            This is a static method so it will influence all InstallItem objects.
            This method is useful in code that does reporting or analyzing, where
            there is need to have access to all oses not just the current or target os.
        """
        _get_for_os = []
        _get_for_os.extend(ItemTable.os_names)

    def reset_get_for_all_oses(self):
        """ resets the list of os that will influence all get functions
            such as depend_list, source_list etc.
            This is a static method so it will influence all InstallItem objects.
            This method is useful in code that does reporting or analyzing, where
            there is need to have access to all oses not just the current or target os.
        """
        self._get_for_os = [self.os_names[0]]

    def begin_get_for_specific_os(self, for_os):
        """ adds another os name to the list of os that will influence all get functions
            such as depend_list, source_list etc.
            This is a static method so it will influence all InstallItem objects.
        """
        self._get_for_os.append(for_os)

    def end_get_for_specific_os(self):
        """ removed the last added os name to the list of os that will influence all get functions
            such as depend_list, source_list etc.
             This is a static method so it will influence all InstallItem objects.
        """
        self._get_for_os.pop()

    def bake_baked_queries(self):
        """ prepare baked queries for later use
        """
        retVal = dict()

        # all queries are now baked just-in-time

        return retVal

    def insert_dicts_to_db(self, item_insert_dicts, details_insert_dicts):
        # self.session.bulk_insert_mappings(SVNRow, insert_dicts)
        self.engine.execute(ItemRow.__table__.insert(), item_insert_dicts)
        self.engine.execute(ItemDetailRow.__table__.insert(), details_insert_dicts)
        self.create_initial_iid_to_detail_relation()

    def get_items(self):  # tested by: TestItemTable.test_ItemRow_get_items
        if "get_all_items" not in self.baked_queries_map:
            the_query = self.bakery(lambda session: session.query(ItemRow))
            the_query += lambda q: q.order_by(ItemRow.iid)
            self.baked_queries_map["get_all_items"] = the_query
        else:
            the_query = self.baked_queries_map["get_all_items"]
        retVal = the_query(self.session).all()
        return retVal

    def get_item(self, iid_to_get):  # tested by: TestItemTable.test_ItemRow_get_item
        if "get_item" not in self.baked_queries_map:
            the_query = self.bakery(lambda q: q.query(ItemRow))
            the_query += lambda  q: q.filter(ItemRow.iid == bindparam("_iid"))
            the_query += lambda q: q.order_by(ItemRow.iid)
            self.baked_queries_map["get_item"] = the_query
        else:
            the_query = self.baked_queries_map["get_item"]
        retVal = the_query(self.session).params(_iid=iid_to_get).first()
        return retVal

    def get_all_iids(self):
        if "get_all_iids" not in self.baked_queries_map:
            the_query = self.bakery(lambda q: q.query(ItemRow.iid))
            the_query += lambda q: q.order_by(ItemRow.iid)
            self.baked_queries_map["get_all_iids"] = the_query
        else:
            the_query = self.baked_queries_map["get_all_iids"]
        retVal = the_query(self.session).all()
        retVal = [m[0] for m in retVal]
        return retVal

    def get_item_by_resolve_status(self, iid_to_get, resolve_status):  # tested by: TestItemTable.test_get_item_by_resolve_status
        # http://stackoverflow.com/questions/29161730/what-is-the-difference-between-one-and-first
        if "get_item_by_resolve_status" not in self.baked_queries_map:
            the_query = self.bakery(lambda q: q.query(ItemRow))
            the_query += lambda  q: q.filter(ItemRow.iid == bindparam("_iid"),
                                             ItemRow.inherit_resolved == bindparam("_resolved"))
            self.baked_queries_map["get_item_by_resolve_status"] = the_query
        else:
            the_query = self.baked_queries_map["get_item_by_resolve_status"]
        retVal = the_query(self.session).params(_iid=iid_to_get, _resolved=resolve_status).first()
        return retVal

    def get_items_by_resolve_status(self, resolve_status):  # tested by: TestItemTable.test_get_items_by_resolve_status
        if "get_items_by_resolve_status" not in self.baked_queries_map:
            the_query = self.bakery(lambda q: q.query(ItemRow))
            the_query += lambda  q: q.filter(ItemRow.inherit_resolved == bindparam("_resolved"))
            the_query += lambda q: q.order_by(ItemRow.iid)
            self.baked_queries_map["get_items_by_resolve_status"] = the_query
        else:
            the_query = self.baked_queries_map["get_items_by_resolve_status"]
        retVal = the_query(self.session).params(_resolved=resolve_status).all()
        return retVal

    def get_original_details_for_item(self, iid):  # tested by: TestItemTable.test_get_original_details_for_item
        if "get_original_details_for_item" not in self.baked_queries_map:
            the_query = self.bakery(lambda session: session.query(ItemDetailRow))
            the_query += lambda q: q.filter(ItemDetailRow.origin_iid == bindparam('iid'))
            #the_query += lambda q: q.filter(ItemDetailRow.os.in_([bindparam('get_for_os')]))
            the_query += lambda q: q.order_by(ItemDetailRow._id)
            self.baked_queries_map["get_original_details_for_item"] = the_query
        else:
            the_query = self.baked_queries_map["get_original_details_for_item"]

        retVal = the_query(self.session).params(iid=iid, get_for_os=self._get_for_os).all()
        return retVal

    def create_initial_iid_to_detail_relation(self):
        retVal = self.session.query(ItemRow.iid, ItemDetailRow._id).filter(ItemRow.iid == ItemDetailRow.origin_iid).all()
        tup_to_dict = [{'iid': iid, 'detail_row': row} for iid, row in retVal]
        self.engine.execute(ItemToDetailRelation.__table__.insert(), tup_to_dict)
        return retVal

    def get_details(self):

        # get_all_details: return all items either files dirs or both, used by get_items()
        if "get_all_details" not in self.baked_queries_map:
            self.baked_queries_map["get_all_details"] = self.bakery(lambda session: session.query(ItemDetailRow))
            self.baked_queries_map["get_all_details"] += self.bakery(lambda session: session.filter(ItemDetailRow.os.in_([bindparam('get_for_os')])))
        retVal = self.baked_queries_map["get_all_details"](self.session).params().all(get_for_os=self._get_for_os)
        return retVal

    def get_item_to_detail_relations(self, what="any"):

        # get_all_details: return all items either files dirs or both, used by get_items()
        if "get_item_to_detail_relations" not in self.baked_queries_map:
            self.baked_queries_map["get_item_to_detail_relations"] = self.bakery(lambda session: session.query(ItemToDetailRelation))
            self.baked_queries_map["get_item_to_detail_relations"] += lambda q: q.order_by(ItemToDetailRelation.iid)

        retVal = self.baked_queries_map["get_item_to_detail_relations"](self.session).all()
        return retVal

    def get_all_details_for_item(self, iid):
        if "get_all_details_for_item" not in self.baked_queries_map:
            the_query = self.bakery(lambda session: session.query(ItemDetailRow))
            the_query += lambda q: q.filter(ItemDetailRow.origin_iid == bindparam('iid'))
            the_query += lambda q: q.filter(ItemDetailRow.os.in_([bindparam('get_for_os')]))
            the_query += lambda q: q.order_by(ItemToDetailRelation._id)
            self.baked_queries_map["get_all_details_for_item"] = the_query
        else:
            the_query = self.baked_queries_map["get_all_details_for_item"]

        retVal = the_query(self.session).params(iid=iid, get_for_os=self._get_for_os).all()
        retVal = [mm[0] for mm in retVal]
        return retVal

    def get_specific_details_for_item(self, iid, detail_name):
        # cannot use baked queries as they do not support array parameters
        the_query = self.session.query(ItemDetailRow.detail_value)\
            .filter(ItemDetailRow.origin_iid == iid,
                    ItemDetailRow.detail_name == detail_name,
                    ItemDetailRow.os.in_(self._get_for_os))
        retVal = the_query.all()
        retVal = [mm[0] for mm in retVal]
        return retVal

    def get_specific_details_for_item2(self, iid, detail_name):
        if "get_specific_details_for_item" not in self.baked_queries_map:
            the_query = self.bakery(lambda session: session.query(ItemDetailRow))
            the_query += lambda q: q.filter(ItemDetailRow.origin_iid == bindparam('iid'))
            the_query += lambda q: q.filter(ItemDetailRow.os.in_([bindparam('get_for_os')]))
            the_query += lambda q: q.filter(ItemDetailRow.detail_name == bindparam('detail_name'))
            self.baked_queries_map["get_specific_details_for_item"] = the_query
        else:
            the_query = self.baked_queries_map["get_specific_details_for_item"]

        retVal = self.session.execute(text(the_query), iid=iid, get_for_os=self._get_for_os, detail_name=detail_name)

        #retVal = the_query(self.session).params(iid=iid, get_for_os=self._get_for_os, detail_name=detail_name).all()
        retVal = [mm[0] for mm in retVal]
        return retVal

    def resolve_item_inheritance(self, item_to_resolve):
        if not item_to_resolve.inherit_resolved:
            inherit_from = [self.get_specific_details_for_item(item_to_resolve.iid, 'inherit')]
            if len(inherit_from) > 0:
                for i_f in inherit_from:
                    self.resolve_iid_inheritance(i_f)
                    detail_rows_for_item = self.get_all_details_rows_for_item(i_f)
                    for d_r in detail_rows_for_item:
                        #if d_f_i.detail_name not in ItemTable.dont_inherit_details:
                            new_d_r = {'iid': item_to_resolve.iid, 'detail_row': d_r}
                            self.engine.execute(ItemToDetailRelation.__table__.insert(), new_d_r)
            item_to_resolve.inherit_resolved = True

    def resolve_iid_inheritance(self, iid_to_resolve):
        item = self.get_item_by_resolve_status(iid_to_resolve, False)
        if item is not None:
            self.resolve_item_inheritance(item)

    def resolve_inheritance(self):
        unresolved_items = self.get_unresolved_items()
        for unresolved_item in unresolved_items:
            self.resolve_item_inheritance(unresolved_item)

    def add_something(self):
        to_add = {'iid': "ADD", 'os': "ADD-os", 'detail_name': "ADD-detail_name", 'detail_value': "ADD-details_value", 'inherited': True}
        self.engine.execute(ItemDetailRow.__table__.insert(), to_add)

if __name__ == "__main__":
    reader = ItemTableYamlReader()

    reader.read_yaml_file('/repositories/betainstl/svn/instl/index.yaml')
    #reader.read_yaml_file('/Users/shai/Desktop/sample_index.yaml')
    #print("\n".join([str(item) for item in reader.items]))
    #print("\n".join([str(detail) for detail in reader.details]))
    it = ItemTable()
    it.insert_dicts_to_db(reader.items, reader.details)
    it.resolve_inheritance()
    print("\n".join([str(item) for item in it.get_items()]))
    print("----")
    print("\n".join([str(detail) for detail in it.get_details()]))
    print("----")
    print("\n".join([str(detail_relation) for detail_relation in it.get_item_to_detail_relations()]))

    #it.add_something()
    print("----\n----")
    #items = it.get_all_iids()
    #print(type(items[0]), items)
    #it.resolve_inheritance()
    #print("\n".join([str(item) for item in it.get_items()]))
    #print("----")
    #print("\n".join([str(detail) for detail in it.get_details()]))
    #print("----\n----")


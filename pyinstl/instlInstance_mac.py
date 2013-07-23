#!/usr/bin/env python2.7
from __future__ import print_function

import os

import instlInstanceBase
import configVar
from copyCommander import CopyCommander_mac_rsync

def quoteme(to_qoute):
    return "".join( ('"', to_qoute, '"') )

class InstlInstance_mac(instlInstanceBase.InstlInstanceBase):
    def __init__(self, initial_vars=None):
        super(InstlInstance_mac, self).__init__(initial_vars)
        self.var_replacement_pattern = "${\g<var_name>}"

    def get_install_instructions_prefix(self):
        return ("#!/bin/sh", "SAVE_DIR=`pwd`")

    def get_install_instructions_postfix(self):
        retVal = list()
        retVal.extend( self.change_directory_cmd("$(SAVE_DIR)") )
        retVal.append("exit 0")
        return retVal

    def make_directory_cmd(self, directory):
        mk_command = " ".join( ("mkdir", "-p", quoteme(directory) ) )
        return (mk_command, )

    def change_directory_cmd(self, directory):
        cd_command = " ".join( ("cd", quoteme(directory) ) )
        return (cd_command, )

    def get_svn_folder_cleanup_instructions(self):
        return 'find . -maxdepth 1 -mindepth 1 -type d -print0 | xargs -0 "$(SVN_CLIENT_PATH)" cleanup --non-interactive'
    
    def create_var_assign(self, identifier, value):
        return identifier+'="'+value+'"'

    def create_echo_command(self, message, file=None):
        echo_command = " ".join(('echo', quoteme(message)))
        if file:
            echo_command = " ".join((echo_command, ">>", file))
        return echo_command

    def create_remark_command(self, remark):
        remark_command = " ".join(('#', quoteme(remark)))
        return remark_command

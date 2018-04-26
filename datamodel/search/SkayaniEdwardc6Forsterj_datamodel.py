'''
Created on Oct 20, 2016
@author: Rohan Achar
'''
from rtypes.pcc.attributes import dimension, primarykey, predicate
from rtypes.pcc.types.subset import subset
from rtypes.pcc.types.set import pcc_set
from rtypes.pcc.types.projection import projection
from rtypes.pcc.types.impure import impure
from datamodel.search.server_datamodel import Link, ServerCopy

@pcc_set
class SkayaniEdwardc6ForsterjLink(Link):
    USERAGENTSTRING = "SkayaniEdwardc6Forsterj"

    @dimension(str)
    def user_agent_string(self):
        return self.USERAGENTSTRING

    @user_agent_string.setter
    def user_agent_string(self, v):
        # TODO (rachar): Make it such that some dimensions do not need setters.
        pass


@subset(SkayaniEdwardc6ForsterjLink)
class SkayaniEdwardc6ForsterjUnprocessedLink(object):
    @predicate(SkayaniEdwardc6ForsterjLink.download_complete, SkayaniEdwardc6ForsterjLink.error_reason)
    def __predicate__(download_complete, error_reason):
        return not (download_complete or error_reason)


@impure
@subset(SkayaniEdwardc6ForsterjUnprocessedLink)
class OneSkayaniEdwardc6ForsterjUnProcessedLink(SkayaniEdwardc6ForsterjLink):
    __limit__ = 1

    @predicate(SkayaniEdwardc6ForsterjLink.download_complete, SkayaniEdwardc6ForsterjLink.error_reason)
    def __predicate__(download_complete, error_reason):
        return not (download_complete or error_reason)

@projection(SkayaniEdwardc6ForsterjLink, SkayaniEdwardc6ForsterjLink.url, SkayaniEdwardc6ForsterjLink.download_complete)
class SkayaniEdwardc6ForsterjProjectionLink(object):
    pass

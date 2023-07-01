# -*- coding: utf-8 -*-
"""
Created on Wed Dec 23 15:11:11 2020

@author: Youri.Baeyens
"""
import pandas as pd

class connectedComponentsLabeler(object):

    """
    A tool to identify the connected components of an undirected graph. 
    """

    hash2seq = {}
    seq2hash = {}
    hashes = []
    edges = pd.DataFrame({})
    numberOfNodes = 0
    forest = []

    def __init__(self, edges):
    
        """
        :parameter edges:  Edges DataFame (two columns named "a" and "b"). 
        
        """
        self.edges = edges

        self.hashes = [h for h in set(self.edges["a"]).union(self.edges["b"])]

        for (hash, seq) in zip(self.hashes, range(len(self.hashes))):
            self.hash2seq[hash] = seq
            self.seq2hash[seq] = hash

        self.numberOfNodes = len(self.hashes)

        self.forest = [i for i in range(0, self.numberOfNodes)]

        for row in self.edges.itertuples():
            self.link(self.hash2seq[row.a], self.hash2seq[row.b])

    def connectedComponentIdentifier(self, node):
    
        """
        Identifies the connected component of a specific node.

        """
        
        if self.forest[node] != node:
            r = self.connectedComponentIdentifier(self.forest[node])
            self.forest[node] = r
            return r
        else:
            return node

    def link(self, nodeA, nodeB):
        """
        Link two buildings.

        If building A is linked with B, then they belong to the same
        connected component.

        If connectedComponentIdentifier(A) != connectedComponentIdentifier(B)
        then we have to correct connectedComponentIdentifier().

        This function actually "corrects" connectedComponentIdentifier


        """
        ccA = self.connectedComponentIdentifier(nodeA)
        ccB = self.connectedComponentIdentifier(nodeB)
        if ccA != ccB:
            self.forest[ccA] = ccB

    def simplifyForest(self):
        """
        Transform the forest into a list.

        Function simplifyForest is used to store the
        connectedComponentIdentifier of every node in forest.

        """
        for i in range(0, len(self.forest)):
            self.forest[i] = self.connectedComponentIdentifier(i)

    def getConnectedCompontents(self):

        self.simplifyForest()

        return pd.DataFrame({'id': self.hashes, 'cc': self.forest})

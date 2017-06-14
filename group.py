from datetime import datetime
from sets import Set
from pyactor.context import interval
from pyactor.context import set_context, create_host, sleep, shutdown, serve_forever
from peer import *
from random import sample




class Group(object):
	_tell = ['announce', 'comprovarPeers', 'init_start', 'infopeer', 'esborrar', 'esborrarId', 'setVotant', 'afegirPeer']
	_ask = ['getMembers', 'getNId', 'getVotant', 'getPeerIds']
	_ref = ['announce', 'esborrar', 'getMembers', 'afegir_host']

	def __init__(self):
		self.tDictionary = {}
		self.nId = -1
		self.votant = False
		self.peerId = {}

	def init_start(self):
		self.interval_check = interval(self.host, 1, self.proxy, 'comprovarPeers')
		
	def afegirPeer(self, idPeer, urlPeer):
		try:
			self.peerId[idPeer] = urlPeer
		except:
			self.peerId = {idPeer:urlPeer}

	def announce(self, urlPeer):
		try:
			self.tDictionary[urlPeer] = datetime.now()
		except:
			self.tDictionary = {urlPeer:datetime.now()}
			
	def getVotant(self):
		return self.votant
		
	def setVotant(self, bolea):
		self.votant = bolea

	def esborrar(self, urlPeer):
		try:
			#if refPeer.timeStamp != -1:
			#	timeStamp = refPeer.timeStamp
			#	newSeq = choice(self.getMembers())
			#	newSeq.setTimeStamp(timeStamp)
			#	for peer in self.getMembers():
			#		peer.afegir_sequencer(newSeq)
			
			del(self.tDictionary[urlPeer])
		except:
			None
	
	def esborrarId(self, idPeer):
		try:
			
			del(self.peerId[idPeer])
		except:
			None

	def getMembers(self):
		try:
			keys = self.tDictionary.keys()
			return keys
		except Exception as e:
            		return []

	def getPeerIds(self):
		return self.peerId

	def comprovarPeers(self):
		tactual = datetime.now()
		
		for urlPeer, t_peer in self.tDictionary.items():
			result=tactual-t_peer

			if result.total_seconds() <= 20:
				None
			else:
				try:
					peer = self.host.lookup_url(urlPeer, Peer)
					print "eliminar "+urlPeer
					self.esborrar(urlPeer)
					self.esborrarId(peer.getId())
					peer.stop_interval()
					peer.stopPeer()
				except TimeoutError as e:
					None

	def infopeer(self):
		print "//////////////////////////////////"
		for peerUrl in self.tDictionary.keys():
			try:
				x = self.host.lookup_url(peerUrl, Peer)
				print " -"+x.getId()+": "
				print x.getMessage()
				noProc = x.getFalten(future=True)
				noProc = noProc.result()
				print noProc
			except TimeoutError as e:
				None
		print "//////////////////////////////////\n"
		
	def getNId(self):
		self.nId = self.nId+1
		return self.nId
		

if __name__ == "__main__":
	set_context()
	host = create_host('http://127.0.0.1:6969')
	
	g = host.spawn('group', Group)
	g.init_start()
	
	serve_forever()


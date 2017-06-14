from pyactor.context import interval, later, set_context, create_host, shutdown
from random import choice
from random import randint
from Queue import PriorityQueue
from pyactor.exceptions import TimeoutError
from group import *


class Peer(object):
	_tell = ['init_start', 'afegir_group', 'join', 'stop_interval', 'leave', 'multicast', 'receive', 'process_msg', 'afegir_sequencer', 'setTimeStamp', 'voteNewSeq', 'stopPeer', 'multicast2']
	_ask = ['getId', 'getMessage', 'getFalten', 'getTimeStamp', 'getVote', 'getNextMessage', 'getUrl']
	_ref = ['afegir_group', 'afegir_host', 'leave', 'afegir_sequencer']

	def __init__(self):
		self.data = []
		self.falten = PriorityQueue()
		self.nextMessage = 0
		self.timeStamp = -1
		self.nId = 0
		self.url = None
		self.seqId = 0

	def init_start(self, url):
		self.url = url
		self.nId = self.group.getNId()
		self.group.afegirPeer(self.nId, self.url)
		self.interval1 = interval(self.host, 3, self.proxy, 'join')

	def stop_interval(self):
		self.interval1.set()

	def leave(self):
		self.stop_interval()
		self.group.esborrar(self.url)
		self.group.esborrarId(self.nId)
		self.host.stop_actor(self.id)

	def join(self):
		self.group.announce(self.url)

	def afegir_group(self, group):
		self.group = group
		
	def afegir_sequencer(self, sequencer, seqId):
		seq = host.lookup_url(sequencer, Peer, future=True)
		seq = seq.result()
		self.sequencer = seq
		self.seqId = seqId

	def getId(self):
		return self.id

	def getMessage(self):
		return self.data
		
	def getFalten(self):
		return self.falten.qsize()
	
	def multicast2(self, message):
		#print self.nId
		#print self.seqId
		votacio = False
		if(self.nId!=self.seqId):
			#print "NO"
			try:
				order = self.sequencer.getTimeStamp()
		
			except TimeoutError as e:
				if(self.group.getVotant() == False):
					self.group.setVotant(True)
					print('----------------------')
					print('Votant nou sequencer')
					print('----------------------')
					self.voteNewSeq()
					sleep(3)
					votacio = True
				
		else:
			#print "SI"
			order = self.getTimeStamp()
		
		sleep(15)
		if(votacio == False):
			for peerUrl in self.group.getMembers():
				try:
					peer = self.host.lookup_url(peerUrl, Peer, future=True)
					peer = peer.result()
					#print message
					#print order
					peer.receive(order, message)
				except TimeoutError as e:
					None
		else:
			None
	
	def multicast(self, message):
		#print self.nId
		#print self.seqId
		votacio = False
		if(self.nId!=self.seqId):
			#print "NO"
			try:
				order = self.sequencer.getTimeStamp()
		
			except TimeoutError as e:
				if(self.group.getVotant() == False):
					self.group.setVotant(True)
					print('----------------------')
					print('Votant nou sequencer')
					print('----------------------')
					self.voteNewSeq()
					sleep(3)
					votacio = True
				
		else:
			#print "SI"
			order = self.getTimeStamp()
		
		if(votacio == False):
			for peerUrl in self.group.getMembers():
				try:
					peer = self.host.lookup_url(peerUrl, Peer, future=True)
					peer = peer.result()
					#print message
					#print order
					peer.receive(order, message)
				except TimeoutError as e:
					None
		else:
			None
			
	def voteNewSeq(self):
		peerId = self.nId
		nouSeq = self
		ordre = self.nextMessage
		guanyo = False
		peers = self.group.getPeerIds().items()
		peers = sorted(peers, reverse=True)
		
		for peerId, peerUrl in peers:
			try:
				#print peerId
				if(peerId != self.nId):
					peer = self.host.lookup_url(peerUrl, Peer)
					peerIdAux = peer.getVote()
					ordreAux = peer.getNextMessage()
					nouSeqUrl = peerUrl
					nouSeqId = peerId
					break
				else:
					guanyo = True
					break
			except:
				None
			
		if(guanyo):
			ordre = ordre-1
			nouSeq.setTimeStamp(ordre)
			nouSeqUrl=self.url
			nouSeqId=self.nId
		else:
			ordreAux = ordreAux-1
			peer.setTimeStamp(ordreAux)
		for peerUrl in self.group.getMembers():
			try:
				peer = self.host.lookup_url(peerUrl, Peer, future=True)
				peer = peer.result()
				peer.afegir_sequencer(nouSeqUrl, nouSeqId)
			except TimeoutError as e:
				None
		self.group.setVotant(False)
			
	def receive(self, order, message):
		if(self.nextMessage == order):
			self.process_msg(message)
		else:
			self.falten.put((order, message))
	
	def process_msg(self, message):
		self.data.append(message)
		self.nextMessage+=1
		if not self.falten.empty():
			msg = self.falten.get()
			if (msg[0] == self.nextMessage):
				self.process_msg(msg[1])
			else:
				self.falten.put(msg)
		
	def getTimeStamp(self):
		self.timeStamp+=1
		return self.timeStamp
		
	def setTimeStamp(self, newTS):
		self.timeStamp = newTS
		
	def getVote(self):
		return self.nId
		
	def getNextMessage(self):
		return self.nextMessage
		
	def stopPeer(self):
		self.host.stop_actor(self.id)
		
	def getUrl(self):
		return self.url
		

if __name__ == "__main__":
	set_context()
	url = 'http://127.0.0.1:6970/'
	host = create_host(url)
	
	numPeers = raw_input("Quants peers vols al sistema? (Sense contar al sequencer)\n")
	
	peers = []
	
	g = host.lookup_url('http://127.0.0.1:6969/group', Group, future=True)
	g = g.result()
	
	seqId = 0
	seqUrl = url+'seq'
	seq = host.spawn('seq', Peer)
	peers.append(seq)
	peers[0].afegir_group(g)
	peers[0].afegir_sequencer(seqUrl, seqId)
	peers[0].init_start(seqUrl)
	#print peers[0].getUrl()
	
	for i in range (1, int(numPeers)+1):
		nom = 'peer'+str(i)
		peerUrl = url+nom
		peers.append(host.spawn(nom, Peer))
		peers[i].afegir_group(g)
		peers[i].afegir_sequencer(seqUrl, seqId)
		peers[i].init_start(peerUrl)
		#print peerUrl
	
	for n in range (1, 1000):
		sleep(3)
		g.infopeer()
		x = raw_input("que vols fer? (1-enviar missatge,  2-enviar missatge amd delay,  3-fer sortir peer del sistema,   4-aturar peer directament,  5-info dels peers,  6-sortir\n")
		
		if(int(x) == 1):
			nomPeer = raw_input("diguem quin peer vols que envii el missatge (0-sequencer inicial, 1-peer1, 2-peer2, ...\n")
			missatge = raw_input("diguem quin missatge vols enviar\n")
			peers[int(nomPeer)].multicast(missatge)
			
		if(int(x) == 2):
			nomPeer = raw_input("diguem quin peer vols que envii el missatge (0-sequencer inicial, 1-peer1, 2-peer2, ...\n")
			missatge = raw_input("diguem quin missatge vols enviar\n")
			peers[int(nomPeer)].multicast2(missatge)
		
		if(int(x) == 3):
			sPeer = raw_input("diguem quin peer vols fer sortir del sistema\n")
			peers[int(sPeer)].leave()
			
		if(int(x) == 4):
			esbPeer = raw_input("diguem quin peer vols esborrar\n")
			peers[int(esbPeer)].stopPeer()
			
		if(int(x) == 5):
			g.infopeer()
			
		if(int(x) == 6):
			shutdown()
			break
			
		#g.infopeer()
	
shutdown()

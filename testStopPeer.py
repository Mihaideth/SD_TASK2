from pyactor.context import interval
from pyactor.context import set_context, create_host, sleep, shutdown
from peer import *
from group import *


if __name__ == "__main__":
	set_context()
	url = 'http://127.0.0.1:6971/'
	host = create_host(url)
	
	g = host.lookup_url('http://127.0.0.1:6969/group', Group, future=True)
	g = g.result()
	
	seq = host.lookup_url('http://127.0.0.1:6970/seq', Peer, future=True)
	seq = seq.result()
	
	p1 = host.lookup_url('http://127.0.0.1:6970/peer1', Peer, future=True)
	p1 = p1.result()

	p2 = host.lookup_url('http://127.0.0.1:6970/peer2', Peer, future=True)
	p2 = p2.result()

	print("Aqui tenim tots els peers")
	g.infopeer()
	sleep(3)
	print("Aturem el peer1")
	p1.stopPeer()
	sleep(3)
	g.infopeer()
	sleep(20)
	g.infopeer()
	sleep(10)
	
shutdown()

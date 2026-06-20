export class LoadBalancer {
  servers: string[];

  VIRTUAL_NODES = 3;
  ring: Node[] = [];

  constructor(serverUrls: string[], virtualNodes = 3) {
    this.servers = serverUrls;
    this.VIRTUAL_NODES = virtualNodes;

    for (const server in this.servers) {
      for (let i = 0; i < this.VIRTUAL_NODES; i++) {
        this.ring.push({ hash: this.getHash(server + `_${i}`), url: server });
      }
    }

    this.ring.sort((a: Node, b: Node): boolean => {
      return a.hash < b.hash;
    });
  }

  getHash = (key: string): number => {
    let arr = key.split("");
    return arr.reduce(
      (hashCode: number, currentVal: string) =>
        (hashCode =
          currentVal.charCodeAt(0) +
          (hashCode << 6) +
          (hashCode << 16) -
          hashCode),
      0,
    );
  };

  start = (): void => {};
}

interface Node {
  hash: number;
  url: string;
}

package mr

type File struct {
	FileName            string
	NetAddressToGetFile string
	RPCName             string
	IsLocal             bool
}

func CreateFile(fileName, netAddress, rpcName string, isLocal bool) File {
	return File{
		FileName:            fileName,
		NetAddressToGetFile: netAddress,
		RPCName:             rpcName,
		IsLocal:             isLocal,
	}
}

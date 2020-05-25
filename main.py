import ext3Worker
from sys import argv


def findAllDeletedFiles(worker, directory):
	deletedFiles = []
	for record in directory:
		if record["deleted"] == 1 and record["fileType"] == 1:
			deletedFiles.append(record)
		if record["deleted"] != 1 and record["fileType"] == 2:
			if record["name"] != "." and record["name"] != "..":
				deletedFiles = deletedFiles + findAllDeletedFiles(worker, worker.getPath(record["inode"]))
	return deletedFiles


def restoreFiles(argv):
	try:
		worker = ext3Worker.Ext3FsWorker(argv[1])
		rootDirectory = worker.getPath(2)
		deletedFiles = findAllDeletedFiles(worker, rootDirectory)
	except Exception:
		print("Storage is not ext3 or is damaged")
		return

	if len(deletedFiles) > 0:
		print("There are " + str(len(deletedFiles)) + " deleted files in " + argv[1])
		for file in deletedFiles:
			print(file["name"] + " inode: " + str(file["inode"]))

	try:
		fsJournal = worker.getJournal()
		fsJournal.reverse()
	except Exception:
		print("Journal is damaged")
		return

	for file in range(len(deletedFiles)):
		flag = 0
		blockOfInode = worker.getBlockOfInode(deletedFiles[file]["inode"])
		for record in fsJournal:
			if record[1] == blockOfInode[0]:
				inodeInJournal = worker.getInode(deletedFiles[file]["inode"], record[0])
				if len(inodeInJournal["blocks"]) > 0:
					flag = 1
					fileRaw = worker.readFileFromInode(inodeInJournal)
					f = open(str(file) + "_" + deletedFiles[file]["name"], "wb")
					f.write(fileRaw)
					f.close()
					break
		if flag == 0:
			f = open("NOT RESTORED " + deletedFiles[file]["name"] + ".txt", "w")
			f.write("File can't be restored: journal record wasn't found\nInode info:\n" + str(worker.getInode(deletedFiles[file]["inode"])))
			f.close()


def printJournal(argv):
	try:
		worker = ext3Worker.Ext3FsWorker(argv[1])
		worker.printJournal()
	except Exception:
		print("Storage is not ext3 or is damaged")
		return


def printInode(argv):
	try:
		worker = ext3Worker.Ext3FsWorker(argv[1])
		worker.printInode(int(argv[3]))
	except Exception:
		print("Storage is not ext3 or is damaged")
		return


def printFsInfo(argv):
	try:
		worker = ext3Worker.Ext3FsWorker(argv[1])
		worker.printSuperBlockInfo()
		worker.printDescriptorsTable()
	except Exception:
		print("Storage is not ext3 or is damaged")
		return


def printJournalBlock(argv):
	try:
		worker = ext3Worker.Ext3FsWorker(argv[1])
		print(worker.getJournalBlock(int(argv[3])))
	except Exception:
		print("Storage is not ext3 or is damaged")
		return


def printJournalInode(argv):
	try:
		worker = ext3Worker.Ext3FsWorker(argv[1])
		print(worker.getInode(int(argv[3]), int(argv[4])))
	except Exception:
		print("Storage is not ext3 or is damaged")
		return


def main():
	if len(argv) < 2:
		print("First argv must be storage!")
		return
	elif len(argv) == 2:
		restoreFiles(argv)
		return
	elif len(argv) > 2:
		if argv[2] == "j":
			printJournal(argv)
		if argv[2] == "i":
			printInode(argv)
		if argv[2] == "fs":
			printFsInfo(argv)
		if argv[2] == "jb":
			printJournalBlock(argv)
		if argv[2] == "ji":
			printJournalInode(argv)


if __name__ == "__main__":
	main()
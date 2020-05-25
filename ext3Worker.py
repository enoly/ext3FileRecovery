import math
import datetime


class Ext3FsWorker:
    superBlockFields = {
        "numOfInodes": ("int", (0, 4)),
        "numOfBlocks": ("int", (4, 8)),
        "freeBlocks": ("int", (12, 16)),
        "freeInodes": ("int", (16, 20)),
        "startOfGroup0": ("int", (20, 24)),
        "sizeOfBlock": ("int", (24, 28)),
        "blocksPerGroup": ("int", (32, 36)),
        "inodesPerGroup": ("int", (40, 44)),
        "sizeOfInode": ("int", (88, 90)),
        "inodeOfJournal": ("int", (224, 228))
    }

    descriptorTableRecordFields = {
        "blocksMapAddress": ("int", (0, 4)),
        "inodesMapAddress": ("int", (4, 8)),
        "inodesTableAddress": ("int", (8, 12)),
        "freeBlocks": ("int", (12, 14)),
        "freeInodes": ("int", (14, 16)),
        "numOfDirectories": ("int", (16, 18))
    }

    inodeFields = {
        "lowerBitsOfSize": ("hex", (4, 8)),
        "accessTime": ("time", (8, 12)),
        "creationTime": ("time", (12, 16)),
        "modificationTime": ("time", (16, 20)),
        "deletionTime": ("time", (20, 24)),
        "countOfLinks": ("int", (26, 28)),
        "numOfSectors": ("int", (28, 32)),
        "directBlocks": ("hex", (40, 88)),
        "indirectBlock": ("int", (88, 92)),
        "2xIndirectBlock": ("int", (92, 96)),
        "3xIndirectBlock": ("int", (96, 100)),
        "highBitsOfSize": ("hex", (108, 112))
    }

    directoryRecordFields = {
        "inode": ("int", (0, 4)),
        "recordLen": ("int", (4, 6)),
        "nameLen": ("int", (6, 7)),
        "fileType": ("int", (7, 8))
    }

    journalTitle = {
        "type": ("int", (4, 8)),
        "number": ("int", (8, 12))
    }

    journalSuperBlock = {
        "sizeOfBlock": ("int", (12, 16)),
        "lenOfJournal": ("int", (16, 20)),
        "firstBlock": ("int", (20, 24)),
        "firstTrans": ("int", (24, 28)),
        "firstTransBlock": ("int", (28, 32))
    }

    journalDescriptorRecord = {
        "block": ("int", (0, 4)),
        "flags": ("int", (4, 8))
    }

    __blockSize = 4096

    def __init__(self, driveName):
        self.__driveName = driveName
        self.__drive = open(self.__driveName, "rb")
        self.__superBlockInfo = self.__getSuperBlockInfo()
        self.__blockSize = self.__superBlockInfo["sizeOfBlock"]
        self.__descriptorsTable = self.__getDescriptorsTable()

    def __getIntFromBlock(self, block, start, end, order="little"):
        numInBytes = block[start: end]
        return int.from_bytes(numInBytes, order)

    def __getTimeFromBlock(self, block, start, end, order="little"):
        utc = self.__getIntFromBlock(block, start, end, order)
        time = datetime.datetime.fromtimestamp(utc)
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def __getStrFromBlock(self, block, start, end):
        string = block[start: end]
        return string.decode("utf-8", errors="ignore")

    def __getFromBlock(self, type, block, start, end, order="little"):
        if type == "int":
            return self.__getIntFromBlock(block, start, end, order)
        elif type == "hex":
            return block[start: end]
        elif type == "str":
            return self.__getStrFromBlock(block, start, end)
        elif type == "time":
            return self.__getTimeFromBlock(block, start, end, order)
        else:
            return -1

    def __getInfoFromRaw(self, struct, raw, order="little"):
        result = dict.fromkeys(struct.keys())
        for field in result.keys():
            fieldStart, fieldEnd = struct[field][1]
            result[field] = self.__getFromBlock(struct[field][0], raw, fieldStart, fieldEnd, order)
        return result

    def __printStructInfo(self, struct):
        for field in struct.keys():
            print(field + ": " + str(struct[field]))

    def __readBlock(self, number):
        self.__drive.seek(number * self.__blockSize)
        return self.__drive.read(self.__blockSize)

    def __getSuperBlockInfo(self):
        superBlockRaw = self.__readBlock(0)[1024: 2048]
        superBlockInfo = self.__getInfoFromRaw(self.superBlockFields, superBlockRaw)
        # Размер блока на диске хранится как количество разрядов для сдвига влево
        superBlockInfo["sizeOfBlock"] = 1024 << superBlockInfo["sizeOfBlock"]
        return superBlockInfo

    def printSuperBlockInfo(self):
        print("Superblock info\n----------------")
        self.__printStructInfo(self.__superBlockInfo)
        print()

    def __parseDescriptorsTable(self, tableRaw, numOfGroups):
        table = []
        for i in range(numOfGroups):
            # Размер одной записи 32 байта
            recordRaw = tableRaw[32 * i: 32 * (i + 1)]
            table.append(self.__getInfoFromRaw(self.descriptorTableRecordFields, recordRaw))
        return table

    def __getDescriptorsTable(self):
        blockOfDescriptor = self.__superBlockInfo["startOfGroup0"] + 1
        numOfGroups = math.ceil(self.__superBlockInfo["numOfBlocks"] / self.__superBlockInfo["blocksPerGroup"])
        descriptorTableRaw = self.__readBlock(blockOfDescriptor)
        return self.__parseDescriptorsTable(descriptorTableRaw, numOfGroups)

    def printDescriptorsTable(self):
        for i in range(len(self.__descriptorsTable)):
            print("Group " + str(i) + " info\n------------")
            self.__printStructInfo(self.__descriptorsTable[i])
            print()

    def getBlockOfInode(self, inodeNum):
        # Иноды нумеруются с 1
        groupOfInode = (inodeNum - 1) // self.__superBlockInfo["inodesPerGroup"]
        inodesPerBlock = self.__blockSize // self.__superBlockInfo["sizeOfInode"]
        inodeNumInGroup = inodeNum - self.__superBlockInfo["inodesPerGroup"] * groupOfInode - 1
        inodeSeekInGroup = inodeNumInGroup // inodesPerBlock
        inodeSeekInBlock = inodeNumInGroup % inodesPerBlock
        blockOfInode = self.__descriptorsTable[groupOfInode]["inodesTableAddress"] + inodeSeekInGroup
        return blockOfInode, inodeSeekInBlock

    def __getInodeFields(self, inodeNum):
        sizeOfInode = self.__superBlockInfo["sizeOfInode"]
        blockOfInode, inodeSeekInBlock = self.getBlockOfInode(inodeNum)
        inodeRaw = self.__readBlock(blockOfInode)[inodeSeekInBlock * sizeOfInode: (inodeSeekInBlock + 1) * sizeOfInode]
        return self.__getInfoFromRaw(self.inodeFields, inodeRaw)

    def __getInodeFieldFromJournal(self, inodeNum, blockInJournal):
        sizeOfInode = self.__superBlockInfo["sizeOfInode"]
        blockOfInode, inodeSeekInBlock = self.getBlockOfInode(inodeNum)
        inodeRaw = blockInJournal[inodeSeekInBlock * sizeOfInode: (inodeSeekInBlock + 1) * sizeOfInode]
        return self.__getInfoFromRaw(self.inodeFields, inodeRaw)

    def __getInodeSize(self, inode):
        inode["size"] = self.__getIntFromBlock(inode["lowerBitsOfSize"] + inode["highBitsOfSize"], 0, 8)
        inode.pop("lowerBitsOfSize")
        inode.pop("highBitsOfSize")
        return inode

    def __sliceNulls(self, blocks):
        while len(blocks) != 0 and blocks[-1] == 0:
            blocks = blocks[:-1]
        return blocks

    def __parseDirectBlocks(self, block):
        result = []
        for i in range(len(block) // 4):
            result.append(self.__getIntFromBlock(block, 4 * i, 4 * (i + 1)))
        return self.__sliceNulls(result)

    def __parseIndirectBlock(self, block):
        result = []
        if block != 0:
            result = self.__parseDirectBlocks(self.__readBlock(block))
        return result

    def __parse2xIndirectBlock(self, block):
        result = []
        if block != 0:
            indirectBlocks = self.__parseDirectBlocks(self.__readBlock(block))
            for i in indirectBlocks:
                result = result + self.__parseIndirectBlock(i)
        return result

    def __parse3xIndirectBlock(self, block):
        result = []
        if block != 0:
            indirectBlocks = self.__parseDirectBlocks(self.__readBlock(block))
            for i in indirectBlocks:
                result = result + self.__parse2xIndirectBlock(i)
        return result

    def __getInodeBlocks(self, inode):
        inode["blocks"] = self.__parseDirectBlocks(inode["directBlocks"])
        inode["directBlocks"] = inode["blocks"] + []
        inode["blocks"] = inode["blocks"] + self.__parseIndirectBlock(inode["indirectBlock"])
        inode["blocks"] = inode["blocks"] + self.__parse2xIndirectBlock(inode["2xIndirectBlock"])
        inode["blocks"] = inode["blocks"] + self.__parse3xIndirectBlock(inode["3xIndirectBlock"])
        return inode

    def getInode(self, inodeNum, journalBlock=None):
        inode = None
        if journalBlock is None:
            inode = self.__getInodeFields(inodeNum)
        else:
            inode = self.__getInodeFieldFromJournal(inodeNum, self.getJournalBlock(journalBlock))
        inode = self.__getInodeSize(inode)
        inode = self.__getInodeBlocks(inode)
        return inode

    def printInode(self, inodeNum, journalBlock=None):
        print("Inode " + str(inodeNum), end="")
        if journalBlock is None:
            print("\n________")
        else:
            print(" from journal block " + str(journalBlock) + "\n_______________________________")
        self.__printStructInfo(self.getInode(inodeNum, journalBlock))
        print()

    def __to4(self, num):
        while num % 4 != 0:
            num = num + 1
        return num

    def __parsePathBlock(self, block):
        result = []
        raw = self.__readBlock(block)
        start, deleted = 0, 0
        while start < self.__blockSize:
            record = self.__getInfoFromRaw(self.directoryRecordFields, raw[start: start + 8])
            record["name"] = self.__getStrFromBlock(raw, start + 8, start + 8 + record["nameLen"])
            if deleted != 0:
                record["deleted"] = 1
            else:
                record["deleted"] = 0
            if (start + 8 + self.__to4(record["nameLen"])) == deleted:
                deleted = 0
            if (8 + self.__to4(record["nameLen"])) != record["recordLen"]:
                deleted = start + record["recordLen"]
            start = start + 8 + self.__to4(record["nameLen"])
            if record["recordLen"] == 0:
                break
            if record["recordLen"] == len(raw) and record["name"] == "":
                continue
            result.append(record)
        return result

    def getPath(self, pathInode):
        pathList = []
        inode = self.getInode(pathInode)
        for block in inode["blocks"]:
            if block != 0:
                pathList = pathList + self.__parsePathBlock(block)
        return pathList

    def printPath(self, pathInode):
        print("Path\n____")
        for record in self.getPath(pathInode):
            if record["deleted"] == 1:
                print("* ", end="")
            print(record["name"] + " inode: " + str(record["inode"]))
        print()

    def __parseJournalDescriptor(self, blockRaw):
        result = []
        start = 12
        while start < self.__blockSize:
            record = self.__getInfoFromRaw(self.journalDescriptorRecord, blockRaw[start: start + 8], "big")
            result.append(record["block"])
            if record["flags"] & 2 != 0:
                start = start + 8
            else:
                start = start + 24
            if record["flags"] & 8 != 0:
                break
        return result

    def __parseJournalBlock(self, blockRaw, title):
        if title["type"] == 1:
            title["name"] = "Descriptor"
            title["blocks"] = self.__parseJournalDescriptor(blockRaw)
        elif title["type"] == 2:
            title["name"] = "Commit"
        elif title["type"] == 3 or title["type"] == 4:
            title["name"] = "Superblock"
        else:
            title["name"] = "Revoke"
        return title

    def getJournal(self):
        journal, blocks = [], []
        inode = self.__superBlockInfo["inodeOfJournal"]
        journalBlocks = self.getInode(inode)["blocks"]
        for i in range(len(journalBlocks)):
            if journalBlocks[i] == 0:
                continue
            blockRaw = self.__readBlock(journalBlocks[i])
            title = self.__getInfoFromRaw(self.journalTitle, blockRaw, "big")
            if title["type"] < 1 or title["type"] > 5:
                if len(blocks) > 0:
                    journal.append((i, blocks.pop(0)))
            else:
                journalBlock = self.__parseJournalBlock(blockRaw, title)
                journal.append((i, journalBlock))
                if title["type"] == 1:
                    blocks = [] + journalBlock["blocks"]
        return journal

    def printJournal(self):
        journal = self.getJournal()
        print("Journal\n_______")
        for record in journal:
            print(str(record[0]) + ": " + str(record[1]))

    def getJournalBlock(self, num):
        num = self.getInode(self.__superBlockInfo["inodeOfJournal"])["blocks"][num]
        return self.__readBlock(num)

    def readFileFromInode(self, inode):
        fileRaw = bytes()
        for block in inode["blocks"]:
            fileRaw = fileRaw + self.__readBlock(block)
        return fileRaw

    def __del__(self):
        self.__drive.close()
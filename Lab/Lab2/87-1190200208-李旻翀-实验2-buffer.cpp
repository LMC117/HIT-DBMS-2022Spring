/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb
{

	BufMgr::BufMgr(std::uint32_t bufs)
		: numBufs(bufs)
	{
		bufDescTable = new BufDesc[bufs];

		for (FrameId i = 0; i < bufs; i++)
		{
			bufDescTable[i].frameNo = i;
			bufDescTable[i].valid = false;
		}

		bufPool = new Page[bufs];

		int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
		hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

		clockHand = bufs - 1;
	}

	// 将所有脏页写回磁盘，然后释放缓冲池、BufDesc表和哈希表占用的内存
	BufMgr::~BufMgr()
	{
		// 脏页全部写回磁盘
		for (FrameId i = 0; i < numBufs; i++)
		{
			if (bufDescTable[i].dirty && bufDescTable[i].valid)
			{
				bufDescTable[i].file->writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
		}

		// 按指向顺序删除，避免产生空指针
		delete hashTable;	   // 删除页表
		delete[] bufPool;	   // 删除 Buffer Pool
		delete[] bufDescTable; // 删除每一个页框的描述
	}

	// 顺时针旋转时钟算法中的表针，将其指向缓冲池中下一个页框
	void BufMgr::advanceClock()
	{
		clockHand++;
		if (clockHand >= numBufs)
			clockHand %= numBufs;
	}

	// 使用时钟算法分配一个空闲页框
	void BufMgr::allocBuf(FrameId &frame)
	{
		unsigned num = 0;
		while (true)
		{
			// 时针旋转
			advanceClock();
			// 若当前页框不包含有效页面，则直接分配它
			if (!bufDescTable[clockHand].valid)
			{
				frame = clockHand;
				return;
			}
			// 若当前页面引用位为true
			if (bufDescTable[clockHand].refbit)
			{
				bufDescTable[clockHand].refbit = false;
				continue;
			}
			// 若当前页面 pinCnt 不为 0
			if (bufDescTable[clockHand].pinCnt)
			{
				num++;
				if (num == numBufs) // 若缓冲池中所有页框都被pin，则抛出BufferExceededException异常
					throw BufferExceededException();
				else
					continue;
			}
			// 该页框引用位为false，pinCnt 为 0
			frame = clockHand;
			// 若页框中的页面是脏的，则需先将脏页写回磁盘
			if (bufDescTable[clockHand].dirty)
			{
				bufDescTable[clockHand].dirty = false;
				bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
			}
			// 如果被分配的页框中包含一个有效页面，则必须将该页面从页表中删除
			if (bufDescTable[clockHand].valid)
			{
				try
				{
					hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
				}
				catch (HashNotFoundException &)
				{
				}
			}
			break;
		}
	}

	// 上层读取页面
	void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
	{
		FrameId frame_num;
		// 页面在缓冲池中
		try
		{
			hashTable->lookup(file, pageNo, frame_num);
			bufDescTable[frame_num].refbit = true;
			bufDescTable[frame_num].pinCnt++;
			page = frame_num + bufPool; // 通过参数page返回指向该页框的指针
		}
		// 页面不在缓冲池中
		catch (HashNotFoundException &)
		{
			allocBuf(frame_num);						 // 分配一个空闲的页框
			bufPool[frame_num] = file->readPage(pageNo); // 将页面从磁盘读入刚刚分配的空闲页框
			hashTable->insert(file, pageNo, frame_num);	 // 将该页面插入哈希表
			bufDescTable[frame_num].Set(file, pageNo);	 // 调用Set()方法正确设置页框的状态
			page = frame_num + bufPool;					 // 通过参数page返回指向该页框的指针
		}
	}

	// 将缓冲区中包含(file, PageNo)表示的页面所在的页框的pinCnt值减1。
	void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
	{
		FrameId frame_num;
		try
		{
			hashTable->lookup(file, pageNo, frame_num);
		}
		catch (HashNotFoundException &) // 如果该页面不在表中，则什么都不用做。
		{
			return;
		}
		// 该页面在表中
		if (bufDescTable[frame_num].pinCnt > 0)
		{
			bufDescTable[frame_num].pinCnt--;
			if (dirty)
				bufDescTable[frame_num].dirty = true;
		}
		else
			throw PageNotPinnedException(bufDescTable[frame_num].file->filename(), bufDescTable[frame_num].pageNo, frame_num);
	}

	// 扫描页面
	void BufMgr::flushFile(const File *file)
	{
		// 遍历，检索缓冲区中所有属于文件file的页面
		for (FrameId i = 0; i < numBufs; i++)
		{
			if (bufDescTable[i].file == file)
			{
				// 检索到文件file的某个无效页或文件file的某些页面被固定住(pinned)，抛出BadBufferException异常
				if (!bufDescTable[i].valid)
					throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
				if (bufDescTable[i].pinCnt > 0)
				{
					throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
				}
				// 如果页面是脏的，则调用file->writePage()将页面写回磁盘，并将dirty位置为false
				if (bufDescTable[i].dirty)
				{
					bufDescTable[i].file->writePage(bufPool[i]);
					bufDescTable[i].dirty = false;
				}
				// 将页面从哈希表中删除
				hashTable->remove(file, bufDescTable[i].pageNo);
				// 调用BufDesc类的Clear()方法将页框的状态进行重置
				bufDescTable[i].Clear();
			}
		}
	}

	// 分配页面
	void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
	{
		FrameId frame_num;

		Page new_page = file->allocatePage(); // 在file文件中分配一个空闲页面
		allocBuf(frame_num);				  // 在缓冲区中分配一个空闲的页框
		bufPool[frame_num] = new_page;

		pageNo = new_page.page_number(); // 通过pageNo参数返回新分配的页面的页号
		page = frame_num + bufPool;		 // 通过page参数返回指向缓冲池中包含该页面的页框的指针

		hashTable->insert(file, pageNo, frame_num); // 在哈希表中插入一条项目
		bufDescTable[frame_num].Set(file, pageNo);	// 调用Set()方法正确设置页框的状态
	}

	// 从文件file中删除页号为pageNo的页面
	void BufMgr::disposePage(File *file, const PageId PageNo)
	{
		FrameId frame_num;
		// 若该页面在缓冲池中
		try
		{
			hashTable->lookup(file, PageNo, frame_num); // 寻找该页面
			hashTable->remove(file, PageNo);			// 若找到，则从哈希表中删除该页面
			bufDescTable[frame_num].Clear();			// 将该页面所在的页框清空
		}
		catch (HashNotFoundException &) // 若该页面不在缓冲池中，则不做操作
		{
		}
		file->deletePage(PageNo); // 从file中删除该页面
	}

	void BufMgr::printSelf(void)
	{
		BufDesc *tmpbuf;
		int validFrames = 0;

		for (std::uint32_t i = 0; i < numBufs; i++)
		{
			tmpbuf = &(bufDescTable[i]);
			std::cout << "frameNo:" << i << " ";
			tmpbuf->Print();

			if (tmpbuf->valid == true)
				validFrames++;
		}

		std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
	}

}

/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "executor.h"

#include <exceptions/buffer_exceeded_exception.h>
#include <cmath>
#include <ctime>
#include <functional>
#include <iostream>
#include <string>
#include <utility>

#include "file_iterator.h"
#include "page_iterator.h"
#include "storage.h"

using namespace std;

namespace badgerdb
{

  void TableScanner::print() const
  {
    badgerdb::File file = badgerdb::File::open(tableFile.filename());
    for (badgerdb::FileIterator iter = file.begin(); iter != file.end(); ++iter)
    {
      badgerdb::Page page = *iter;
      badgerdb::Page *buffered_page;
      bufMgr->readPage(&file, page.page_number(), buffered_page);

      for (badgerdb::PageIterator page_iter = buffered_page->begin();
           page_iter != buffered_page->end(); ++page_iter)
      {
        string key = *page_iter;
        string print_key = "(";
        int current_index = 0;
        for (int i = 0; i < tableSchema.getAttrCount(); ++i)
        {
          switch (tableSchema.getAttrType(i))
          {
          case INT:
          {
            int true_value = 0;
            for (int j = 0; j < 4; ++j)
            {
              if (std::string(key, current_index + j, 1)[0] == '\0')
              {
                continue; // \0 is actually representing 0
              }
              true_value +=
                  (std::string(key, current_index + j, 1))[0] * pow(128, 3 - j);
            }
            print_key += to_string(true_value);
            current_index += 4;
            break;
          }
          case CHAR:
          {
            int max_len = tableSchema.getAttrMaxSize(i);
            print_key += std::string(key, current_index, max_len);
            current_index += max_len;
            current_index +=
                (4 - (max_len % 4)) % 4; // align to the multiple of 4
            break;
          }
          case VARCHAR:
          {
            int actual_len = key[current_index];
            current_index++;
            print_key += std::string(key, current_index, actual_len);
            current_index += actual_len;
            current_index +=
                (4 - ((actual_len + 1) % 4)) % 4; // align to the multiple of 4
            break;
          }
          }
          print_key += ",";
        }
        print_key[print_key.size() - 1] = ')'; // change the last ',' to ')'
        cout << print_key << endl;
      }
      bufMgr->unPinPage(&file, page.page_number(), false);
    }
    bufMgr->flushFile(&file);
  }

  JoinOperator::JoinOperator(File &leftTableFile,
                             File &rightTableFile,
                             const TableSchema &leftTableSchema,
                             const TableSchema &rightTableSchema,
                             const Catalog *catalog,
                             BufMgr *bufMgr)
      : leftTableFile(leftTableFile),
        rightTableFile(rightTableFile),
        leftTableSchema(leftTableSchema),
        rightTableSchema(rightTableSchema),
        resultTableSchema(
            createResultTableSchema(leftTableSchema, rightTableSchema)),
        catalog(catalog),
        bufMgr(bufMgr),
        isComplete(false)
  {
    // nothing
  }

  TableSchema JoinOperator::createResultTableSchema(
      const TableSchema &leftTableSchema,
      const TableSchema &rightTableSchema)
  {
    vector<Attribute> attrs;

    // first add all the left table attrs to the result table
    for (int k = 0; k < leftTableSchema.getAttrCount(); ++k)
    {
      Attribute new_attr = Attribute(
          leftTableSchema.getAttrName(k), leftTableSchema.getAttrType(k),
          leftTableSchema.getAttrMaxSize(k), leftTableSchema.isAttrNotNull(k),
          leftTableSchema.isAttrUnique(k));
      attrs.push_back(new_attr);
    }

    // test every right table attrs, if it doesn't have the same attr(name and
    // type) in the left table, then add it to the result table
    for (int i = 0; i < rightTableSchema.getAttrCount(); ++i)
    {
      bool has_same = false;
      for (int j = 0; j < leftTableSchema.getAttrCount(); ++j)
      {
        if ((leftTableSchema.getAttrType(j) == rightTableSchema.getAttrType(i)) &&
            (leftTableSchema.getAttrName(j) == rightTableSchema.getAttrName(i)))
        {
          has_same = true;
        }
      }
      if (!has_same)
      {
        Attribute new_attr = Attribute(
            rightTableSchema.getAttrName(i), rightTableSchema.getAttrType(i),
            rightTableSchema.getAttrMaxSize(i), rightTableSchema.isAttrNotNull(i),
            rightTableSchema.isAttrUnique(i));
        attrs.push_back(new_attr);
      }
    }
    return TableSchema("TEMP_TABLE", attrs, true);
  }

  void JoinOperator::printRunningStats() const
  {
    cout << "# Result Tuples: " << numResultTuples << endl;
    cout << "# Used Buffer Pages: " << numUsedBufPages << endl;
    cout << "# I/Os: " << numIOs << endl;
  }

  vector<Attribute> JoinOperator::getCommonAttributes(
      const TableSchema &leftTableSchema,
      const TableSchema &rightTableSchema) const
  {
    vector<Attribute> common_attrs;
    for (int i = 0; i < rightTableSchema.getAttrCount(); ++i)
    {
      for (int j = 0; j < leftTableSchema.getAttrCount(); ++j)
      {
        if ((leftTableSchema.getAttrType(j) == rightTableSchema.getAttrType(i)) &&
            (leftTableSchema.getAttrName(j) == rightTableSchema.getAttrName(i)))
        {
          Attribute new_attr = Attribute(rightTableSchema.getAttrName(i),
                                         rightTableSchema.getAttrType(i),
                                         rightTableSchema.getAttrMaxSize(i),
                                         rightTableSchema.isAttrNotNull(i),
                                         rightTableSchema.isAttrUnique(i));
          common_attrs.push_back(new_attr);
        }
      }
    }
    return common_attrs;
  }

  /**
   * use the original key to generate the search key
   * @param key
   * @param common_attrs
   * @param TableSchema
   * @return
   */
  string construct_search_key(string key,
                              vector<Attribute> &common_attrs,
                              const TableSchema &TableSchema)
  {
    string search_key;
    int current_index = 0;
    int current_attr_index = 0;
    for (int i = 0; i < TableSchema.getAttrCount(); ++i)
    {
      switch (TableSchema.getAttrType(i))
      {
      case INT:
      {
        if (TableSchema.getAttrName(i) ==
                common_attrs[current_attr_index].attrName &&
            TableSchema.getAttrType(i) ==
                common_attrs[current_attr_index].attrType)
        {
          search_key += std::string(key, current_index, 4);
          current_attr_index++;
        }
        current_index += 4;
        break;
      }
      case CHAR:
      {
        int max_len = TableSchema.getAttrMaxSize(i);
        if (TableSchema.getAttrName(i) ==
                common_attrs[current_attr_index].attrName &&
            TableSchema.getAttrType(i) ==
                common_attrs[current_attr_index].attrType)
        {
          search_key += std::string(key, current_index, max_len);
          current_attr_index++;
        }
        current_index += max_len;
        current_index += (4 - (max_len % 4)) % 4;
        ; // align to the multiple of 4
        break;
      }
      case VARCHAR:
      {
        int actual_len = key[current_index];
        current_index++;
        if (TableSchema.getAttrName(i) ==
                common_attrs[current_attr_index].attrName &&
            TableSchema.getAttrType(i) ==
                common_attrs[current_attr_index].attrType)
        {
          search_key += std::string(key, current_index, actual_len);
          current_attr_index++;
        }
        current_index += actual_len;
        current_index +=
            (4 - ((actual_len + 1) % 4)) % 4; // align to the multiple of 4
        break;
      }
      }
      if (current_attr_index >= common_attrs.size())
        break;
    }
    return search_key;
  }

  string JoinOperator::joinTuples(string leftTuple,
                                  string rightTuple,
                                  const TableSchema &leftTableSchema,
                                  const TableSchema &rightTableSchema) const
  {
    int cur_right_index = 0; // current substring index in the right table key
    string result_tuple = leftTuple;

    for (int i = 0; i < rightTableSchema.getAttrCount(); ++i)
    {
      bool has_same = false;
      for (int j = 0; j < leftTableSchema.getAttrCount(); ++j)
      {
        if ((leftTableSchema.getAttrType(j) == rightTableSchema.getAttrType(i)) &&
            (leftTableSchema.getAttrName(j) == rightTableSchema.getAttrName(i)))
        {
          has_same = true;
        }
      }
      // if the key is only owned by right table, add it to the result tuple
      switch (rightTableSchema.getAttrType(i))
      {
      case INT:
      {
        if (!has_same)
        {
          result_tuple += std::string(rightTuple, cur_right_index, 4);
        }
        cur_right_index += 4;
        break;
      }
      case CHAR:
      {
        int max_len = rightTableSchema.getAttrMaxSize(i);
        if (!has_same)
        {
          result_tuple += std::string(rightTuple, cur_right_index, max_len);
        }
        cur_right_index += max_len;
        unsigned align_ = (4 - (max_len % 4)) % 4; // align to the multiple of
                                                   // 4
        for (int k = 0; k < align_; ++k)
        {
          result_tuple += "0";
          cur_right_index++;
        }
        break;
      }
      case VARCHAR:
      {
        int actual_len = rightTuple[cur_right_index];
        result_tuple += std::string(rightTuple, cur_right_index, 1);
        cur_right_index++;
        if (!has_same)
        {
          result_tuple += std::string(rightTuple, cur_right_index, actual_len);
        }
        cur_right_index += actual_len;
        unsigned align_ =
            (4 - ((actual_len + 1) % 4)) % 4; // align to the multiple of 4
        for (int k = 0; k < align_; ++k)
        {
          result_tuple += "0";
          cur_right_index++;
        }
        break;
      }
      }
    }
    return result_tuple;
  }

  bool OnePassJoinOperator::execute(int numAvailableBufPages, File &resultFile)
  {
    if (isComplete)
      return true;

    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0;

    // TODO: Execute the join algorithm

    isComplete = true;
    return true;
  }

  bool NestedLoopJoinOperator::execute(int numAvailableBufPages,
                                       File &resultFile)
  {
    if (isComplete)
      return true;

    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0; // 磁盘 IO 数

    // TODO: Execute the join algorithm

    vector<string> result_list;                                                              // 保存结果
    vector<Attribute> common_attrs = getCommonAttributes(leftTableSchema, rightTableSchema); // 寻找两个表的公共部分
    badgerdb::FileIterator iter = leftTableFile.begin();                                     // 获取S关系的头指针
    while (iter != leftTableFile.end())
    {
      // 将外关系S的M-1个块读入缓存池
      vector<badgerdb::Page *> used_list; // 保存读入缓存的块
      for (int i = 0; i < numAvailableBufPages - 1; i++)
      {
        badgerdb::Page *buffered_page;
        badgerdb::Page page = *iter;

        bufMgr->readPage(&leftTableFile, page.page_number(), buffered_page);
        used_list.push_back(buffered_page);

        numUsedBufPages++; // 更新使用的页面数
        numIOs++;          // 更新IO数

        if (++iter == leftTableFile.end()) // 若S关系中的元组提前读完
          break;
      }

      // 每次读入并处理外关系R中的一个块P
      for (badgerdb::FileIterator iter = rightTableFile.begin(); iter != rightTableFile.end(); iter++)
      {
        badgerdb::Page page = *iter;
        badgerdb::Page *buffered_page;

        bufMgr->readPage(&rightTableFile, page.page_number(), buffered_page);

        numUsedBufPages++;
        numIOs++;

        for (badgerdb::PageIterator page_iter = buffered_page->begin();
             page_iter != buffered_page->end(); ++page_iter)
        {
          string rightKey = *page_iter;
          string key_right_flag = construct_search_key(rightKey, common_attrs, rightTableSchema);
          // 查找能与r元组进行连接的元组s
          for (int i = 0; i < used_list.size(); i++) // 遍历缓存块
          {
            badgerdb::Page *buffered_page_left = used_list.at(i);
            for (badgerdb::PageIterator page_iter_left = buffered_page_left->begin(); // 从头开始关系S的遍历
                 page_iter_left != buffered_page_left->end(); page_iter_left++)
            {
              string result_tuple;
              string leftKey = *page_iter_left;
              string key_left_flag = construct_search_key(leftKey, common_attrs, leftTableSchema); // 寻找含有共同部分的元组

              // 判断是否相等
              if (key_left_flag == key_right_flag)
              {
                result_tuple = joinTuples(leftKey, rightKey, leftTableSchema, rightTableSchema);
                result_list.push_back(result_tuple);
              }
            }
          }
        }
      }
    }

    // 将结果写入文件
    for (int i = 0; i < result_list.size(); i++)
      HeapFileManager::insertTuple(result_list.at(i), resultFile, bufMgr);

    numResultTuples = result_list.size(); // 更新 numResultTuples
    if (numUsedBufPages > numAvailableBufPages)
      numUsedBufPages = numAvailableBufPages; // 使用的缓存块数不能超过 numAvailableBufPages
    else
      numUsedBufPages = numUsedBufPages;

    isComplete = true;
    return true;
  }

  BucketId GraceHashJoinOperator::hash(const string &key) const
  {
    std::hash<string> strHash;
    return strHash(key) % numBuckets;
  }

  bool GraceHashJoinOperator::execute(int numAvailableBufPages,
                                      File &resultFile)
  {
    if (isComplete)
      return true;

    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0;

    // TODO: Execute the join algorithm

    isComplete = true;
    return true;
  }

} // namespace badgerdb
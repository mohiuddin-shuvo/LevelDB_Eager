#include <iostream>
#include <sstream>
#include <string>

#include "leveldb/db.h"
#include "rapidjson/document.h"



std::string generateJSON(std::string k, std::string v) {
    return "{\"City\": \"" + k + "\",\"State\": \"" + v + "\"}";
}

void print_vals(std::vector<leveldb::KeyValuePair>& vals) {
  for(std::vector<leveldb::KeyValuePair>::iterator it = vals.begin(); it != vals.end(); ++it)
    std::cout << "key: " << it->key.data() << " value: " << it->value.data() << std::endl;
}


int main(int argc, char** argv) {


//
//************************************************************************************
leveldb::DB* db;
leveldb::Options options;

options.create_if_missing = true;

options.using_s_index = true;
options.primary_key = "City";
options.secondary_key = "State";

std::cout << "Trying to create database\n";
if (!leveldb::DB::Open(options, "./main/testdb", &db).ok()) return 1;
std::cout << "Created databases\n";


// insert some key-value pairs
leveldb::WriteOptions woptions;
std::string val;

std::cout << "Trying to write values\n";

    val = generateJSON("Riverside", "California");
    leveldb::Status s = db->Put(woptions, val);
    assert(s.ok());
//*
    val = generateJSON("Los Angeles", "California");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("San Diego", "California");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Miami", "Florida");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Springfield", "Illinois");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Springfield", "Massachusetts");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Los Angeles", "California");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Boston", "Massachusetts");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Irvine", "California");
    s = db->Put(woptions, val);
    assert(s.ok());

  //*/
std::cout << "\nFinished writing values\n";




std::cout << "\nDeleting values\n";

val = "Springfield";
s = db->Delete(woptions, val);
assert(s.ok());

val = "Irvine";
s = db->Delete(woptions, val);
assert(s.ok());

val = "Miami";
s = db->Delete(woptions, val);
assert(s.ok());

std::cout << "\nFinished deleting values\n";




std::cout << "\nReading back values\n";

  //* // read them back
leveldb::ReadOptions roptions;
std::string skey;
std::vector<leveldb::KeyValuePair> ret_vals;

    skey = "California";
    roptions.num_records = 5;
    leveldb::Status s2 = db->Get(roptions, skey, &ret_vals);
    assert(s2.ok());
    print_vals(ret_vals);

    ret_vals.clear();
    skey = "Florida";
    roptions.num_records = 2;
    db->Get(roptions, skey, &ret_vals);
    print_vals(ret_vals);

    ret_vals.clear();
    skey = "Illinois";
    roptions.num_records = 4;
    db->Get(roptions, skey, &ret_vals);
    print_vals(ret_vals);

    ret_vals.clear();
    skey = "Massachusetts";
    roptions.num_records = 3;
    db->Get(roptions, skey, &ret_vals);
    print_vals(ret_vals);
//*/
std::cout << "\nFinished reading values\n";


//************************************************************************************
//




#if 0
leveldb::DB* db;
leveldb::Options options;

options.create_if_missing = true;

options.using_s_index = true;
options.primary_key = "City";
options.secondary_key = "State";

if (!leveldb::DB::Open(options, "./testdb", &db).ok()) return 1;


    // insert some key-value pairs
leveldb::WriteOptions woptions;
string val;

    val = generateJSON("Riverside", "California");
    leveldb::Status s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Los Angeles", "California");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("San Diego", "California");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Miami", "Florida");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Springfield", "Illinois");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Springfield", "Massachusetts");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Los Angeles", "California");
    s = db->Put(woptions, val);
    assert(s.ok());


/*    // read them back
leveldb::ReadOptions roptions;
string skey;
vector<leveldb::KeyValuePair> ret_vals;

    skey = "California";
    leveldb::Status s2 = db->Get(roptions, skey, ret_vals);
    assert(s2.ok());
    print_vals(ret_vals);

    ret_vals.clear();
    skey = "Florida";
    db->Get(roptions, skey, ret_vals);
    print_vals(ret_vals);

    ret_vals.clear();
    skey = "Illinois";
    db->Get(roptions, skey, ret_vals);
    print_vals(ret_vals);

    ret_vals.clear();
    skey = "Massachusetts";
    db->Get(roptions, skey, ret_vals);
    print_vals(ret_vals);
//*/

//*
cout << "\nPrimary db contents:\n";
leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());

for (it->SeekToFirst(); it->Valid(); it->Next())
  cout << it->key().data() << " : " << it->value().data() << endl;

if (false == it->status().ok())
  cerr << "An error was found during the scan" << endl << it->status().ToString() << endl;

delete it; 

cout << "\nSecondary db contents:\n";
leveldb::Iterator* sit = db->sdb->NewIterator(leveldb::ReadOptions());

for (sit->SeekToFirst(); sit->Valid(); sit->Next())
  cout << sit->key().data() << " : " << sit->value().data() << endl;

if (false == sit->status().ok())
  cerr << "An error was found during the scan" << endl << it->status().ToString() << endl;

delete sit; //*/

delete db;

#endif

}

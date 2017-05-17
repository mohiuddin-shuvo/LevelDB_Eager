#include <cstdlib>
#include <sstream>
#include <stdlib.h>
#include <cassert>
#include <algorithm>
#include <leveldb/db.h>
#include <leveldb/slice.h>
#include <leveldb/filter_policy.h>
#include <fstream>
#include <iostream>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include <sys/time.h>
#include <unistd.h>

using namespace std;
//NYTweetsUserIDRangerw19
//Small-CreationTime-Write-Range3
static string database = "/home/mohiuddin/Desktop/LevelDB_Correctness_Testing/DB/Embedded_Write_UserID_Sec100_K5";
static string benchmark_file = "/home/mohiuddin/Desktop/LevelDB_Correctness_Testing/Benchmarks/Small-CreationTime-Write-Range3";
static string result_file = "/home/mohiuddin/Desktop/LevelDB_Correctness_Testing/Results/Lazy_Write_UserID_Sec100_K5.csv";

//static int numberofiterations = 2;

static long LOG_POINT = 10000;
static int topk = 5;


std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}

void testWithBenchMark()
{
    leveldb::DB *db;
    leveldb::Options options;
	//////////////////////////////////////////////
	//string rw = "rw19";

	int bloomfilter = 100;
//	string sbloomfilter = "100";

    options.filter_policy = leveldb::NewBloomFilterPolicy(bloomfilter);
    options.primary_key = "ID";
    //options.secondary_key = "UserID";
    options.secondary_key = "CreationTime";
    //options.secondaryAtt = "Hashtags";

    options.create_if_missing = true;
    //options.isSecondaryDB = false;
//    if(isintervaltree)
//    	options.IntervalTreeFileName = "./interval_tree";
//    else
//    	options.IntervalTreeFileName ="";

    leveldb::Status status = leveldb::DB::Open(options, database, &db);
    assert(status.ok());

    ifstream ifile(benchmark_file.c_str());
    vector<leveldb::KeyValuePair> svalues;
    vector<leveldb::RangeKeyValuePair> srangevalues;
    if (!ifile) { cerr << "Can't open input file " << endl; return; }
    string line;
    int i=0;
    rapidjson::Document d;

    leveldb::ReadOptions roptions;
    roptions.num_records = topk;
    double w=0, rs=0, rp=0, rsrange = 0 ;
    long rscount = 0, rsrangecount = 0 ;
    double durationW=0,durationRS=0,durationRP=0, durationRSRange=0 ;
	ofstream ofile(result_file.c_str(),  std::ofstream::out | std::ofstream::app );
	//ofstream ofile1(result_file1.c_str(),  std::ofstream::out | std::ofstream::app );
    while(getline(ifile, line)) {
		i++;

        std::vector<std::string> x = split(line, '\t');
        leveldb::WriteOptions woptions;
		struct timeval start, end;
        if(x.size()>=3) {
            leveldb::Slice key = x[2];
//            if(x[1]=="w")
//            	continue;
//            cout<<x[1]<<endl;
//            cout<<x[2]<<endl;
            gettimeofday(&start, NULL);
            if(x[1]=="w") {
                leveldb::Status s = db->Put(woptions,key);
                gettimeofday(&end, NULL);
                durationW+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
				w++;
            } else if(x[1]=="rs") {

            	int isrange = rand() % 2;
            	//if(!isrange)
            	{
					leveldb::Status s = db->Get(roptions, key , &svalues);
					gettimeofday(&end, NULL);
					durationRS+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
					int size = svalues.size();
					rscount+=size;
					if(svalues.size()>0) {
						svalues.clear();
					}
					rs++;
            	}
            //	else
            	{
            		x[3] = x[3].substr(0, x[3].length()-1);
            		leveldb::Slice ekey =x[3];
            		gettimeofday(&start, NULL);
            		leveldb::Status s = db->RangeLookUp(roptions, x[2] , x[3] , &srangevalues);
					gettimeofday(&end, NULL);
					durationRSRange+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
					int size = srangevalues.size();
					rsrangecount+=size;
					if(srangevalues.size()>0) {
						srangevalues.clear();
					}

					//sleep(2);
					rsrange++;
            	}

                //ofile1<<rs<<", "<<size<<endl;
            } else if(x[1]=="rp") {
                std::string pvalue;
                leveldb::Status s = db->Get(leveldb::ReadOptions(), key , &pvalue);
                gettimeofday(&end, NULL);
                durationRP+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
                rp++;
            }
        }
		if (i%LOG_POINT == 0)
		{
			if(i/LOG_POINT==20)
				break;

			if(i/LOG_POINT==1)
			{
				ofile << "No of Op (Millions)" <<"," << "Time Per Op." <<"," << "Time Per Write" <<"," << "Time Per Read" <<"," << "Time Per Lookup" <<"," << "Results Per Lookup" <<"," << "Time Per RangeLookup" <<"," << "Results Per RangeLookup" <<endl<<endl;
				cout << "No of Op (Millions)" <<"," << "Time Per Write" <<"," << "Time Per Read" <<"," << "Time Per Lookup" <<"," << "Results Per Lookup" <<"," << "Time Per RangeLookup" <<"," << "Results Per RangeLookup" <<endl<<endl;

			}
			cout<< fixed;
			cout.precision(3);
			ofile << i/LOG_POINT <<"," << (durationW+durationRP+durationRS+durationRSRange)/i <<"," << durationW/w  <<"," << durationRP/rp <<"," << durationRS/rs <<"," << rscount/rs <<"," << durationRSRange/rsrange <<"," << rsrangecount/rsrange <<endl<<endl;
		    cout << i/LOG_POINT <<",\t" << (durationW+durationRP+durationRS+durationRSRange)/i <<",\t"
		    		<< durationW/w  <<",\t" << durationRP/rp <<",\t" << durationRS/rs <<",\t"
					<< rscount/rs <<",\t" << durationRSRange/rsrange <<",\t"
					<< rsrangecount/rsrange <<endl<<endl;
		}
    }

    cout << i/LOG_POINT <<",\t" << (durationW+durationRP+durationRS+durationRSRange)/i <<",\t"
    		<< durationW/w  <<",\t" << durationRP/rp <<",\t" << durationRS/rs <<",\t"
			<< rscount/rs <<",\t" << durationRSRange/rsrange <<",\t"
			<< rsrangecount/rsrange <<endl<<endl;


	delete db;
    delete options.filter_policy;


}

void dotestWithBenchMark()
{
    struct timeval start, end;

    gettimeofday(&start, NULL);

    testWithBenchMark();

    gettimeofday(&end, NULL);

//    long long duration = ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));

    //cout<<"Duration: "<<duration/1000000/60.0<<endl;

    //ofstream ofile("/home/schen064/leveldb_experiments/result_embedded_50Mrw91_hashtags_bloomfiler100",std::ofstream::out | std::ofstream::app );

   // ofile<<"Total duration: "<<duration/1000000/60.0<<endl;
}

int main(int argc, char** argv) {


	if(argc == 4)
	{
		benchmark_file = argv[1];
		database = argv[2];
		result_file = argv[3];
		//cout<< argv[3] ;
		dotestWithBenchMark();

	}
	else
	{
		testWithBenchMark();
		cout<<"Please Put arguments in order: \n arg1=benchmarkpath arg2=dbpath arg3=resultpath\n";
	}
	//cout<<"\nasdasd\n";
	//else



	//	string cellstring  = "2.3.5Q";
//	string endkey = "2.3.5";
//	cellstring.substr(0, cellstring.size()-1).compare(endkey) >= 0 ? cout <<"break": cout<<"continue";
	//cout<<"Compile_DualDB!\n";
	return 0;
}


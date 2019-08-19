//
// Created by Qingzhi Ma.
// Department of Computer Science
// University of Warwick
// Email: Q.Ma.2@warwick.ac.uk
// Copyright (c) 2019 University of Warwick. All rights reserved.
//

// SELECT r_name, n_regionkey, s_nationkey, ps_suppkey
// FROM region, nation, supplier, partsupp
// WHERE	r_name = 'ASIA'
// AND r_regionkey = n_regionkey
// AND n_nationkey = s_nationkey;
// ///AND s_suppkey = ps_suppkey;

#include <iostream>
#include <fstream>
#include <memory>
#include <random>
#include <map>
#include <vector>
#include <thread>
#include <future>

#include "util/Timer.h"
#include "util/FileSizeTable.h"
#include "util/FileKeyValue.h"
#include "util/accumulator.h"
#include "util/joinSettings.h"

#include "database/TableRegion.h"
#include "database/TableNation.h"
#include "database/TableSupplier.h"
#include "database/TablePartsupp.h"

#include "database/jefastIndex.h"
#include "database/jefastBuilder.h"

static std::shared_ptr<Table> region_table;
static std::shared_ptr<Table> nation_table;


static std::shared_ptr<jefastIndexLinear> jefastIndex;
static std::shared_ptr<FileKeyValue> data_map;

static global_settings queryTestSettings;



// Note, we will require a filter for each column.  It can just be an empty filter (an everything filter)
// we will be doing a linear scan of the data to implement this algorithm for now.
int64_t exactJoinNoIndex(std::string outfile, std::vector<std::shared_ptr<jefastFilter> > filters) {
    // implements a straightforward implementation of a join which
    // does not require an index.

    std::ofstream output_file(outfile, std::ios::out);
    int64_t count = 0;

    auto Table_1 = region_table;
    auto Table_2 = nation_table;

    int table1Index2 = Table_Region::R_REGIONKEY;
    int table2Index1 = Table_Nation::N_REGIONKEY;


    // build the hash for table 1
    std::map<jfkey_t, std::vector<int64_t> > Table1_hash;
    //int64_t Table_1_count = Table_1->row_count();
    //for (int64_t i = 0; i < Table_1_count; ++i) {
    for (auto f1_enu = filters.at(0)->getEnumerator(); f1_enu->Step();) {
        Table1_hash[Table_1->get_int64(f1_enu->getValue(), table1Index2)].push_back(f1_enu->getValue());
    }

    // build the hash for table 2.  All matched elements from table 1 hash will be emitted
    // the tuple has the form <index from table 1, index for table 2> for all matching tuple
    std::map<jfkey_t, std::vector<std::tuple<int64_t, int64_t> > > Table2_hash;
    //int64_t Table_2_count = Table_2->row_count();
    //for (int64_t i = 0; i < Table_2_count; ++i) {
    for (auto f2_enu = filters.at(1)->getEnumerator(); f2_enu->Step();) {
        jfkey_t value = Table_2->get_int64(f2_enu->getValue(), table2Index1);
//        for (auto matching_indexes : Table1_hash[value]) {
//            Table2_hash[Table_2->get_int64(f2_enu->getValue(), table2Index2)].emplace_back(matching_indexes, f2_enu->getValue());
//        }
    }

    output_file.close();
    return count;
}

void setup_data() {
    // load the tables into memory
    FileSizeTable table_sizes("fileInfo.txt");
    data_map.reset(new FileKeyValue("query0_timings.txt"));
//
    Timer timer;
    std::cout << "opening tables" << std::endl;
    timer.reset();
    timer.start();
    region_table.reset(new Table_Region("region.tbl", table_sizes.get_lines("region.tbl")));
    nation_table.reset(new Table_Nation("nation.tbl", table_sizes.get_lines("nation.tbl")));

    timer.stop();
    std::cout << "opening tables took " << timer.getSeconds() << " seconds" << std::endl;
    data_map->appendArray("opening_tables", long(timer.getMilliseconds()));
//
//    // build the indexes which might be used in the experiment
    std::cout << "building indexes" << std::endl;
    timer.reset();
    timer.start();
//    // join indexes
    if (queryTestSettings.buildIndex) {
        region_table->get_key_index(Table_Region::R_REGIONKEY);
        nation_table->get_key_index(Table_Nation::N_REGIONKEY);
    }
//    // filtering conditions

    // we do not currently have plans to do selection conditions on query 0
    timer.stop();
    std::cout << "done building indexes took " << timer.getMilliseconds() << " milliseconds" << std::endl;
    data_map->appendArray("building_indexes", long(timer.getMilliseconds()));


    // find max outdegree of each join?
}

int main(int argc, char** argv) {
    std::cout<<"Start...";
    queryTestSettings = parse_args(argc, argv);

    setup_data();
    Timer timer;

    std::cout<<"hash join ...";
    // do hash join
    if(queryTestSettings.nonJoinAttribHash)
    {
        std::cout<<"hash join ...";
        std::vector<std::shared_ptr<jefastFilter> > filters(4);
        filters.at(0) = std::shared_ptr<jefastFilter>(new all_jefastFilter(region_table, Table_Region::R_REGIONKEY));
        filters.at(1) = std::shared_ptr<jefastFilter>(new all_jefastFilter(nation_table, Table_Nation::N_NATIONKEY));

        timer.reset();
        timer.start();
        auto count = exactJoinNoIndex("query0_full.txt", filters);
        timer.stop();
        std::cout << "full join took " << timer.getMilliseconds() << " milliseconds with cardinality " << count << std::endl;
        data_map->appendArray("full_join", long(timer.getMilliseconds()));
        data_map->appendArray("full_join_cadinality", count);
    }
}

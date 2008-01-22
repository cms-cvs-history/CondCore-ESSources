//
// Package:    CondCore/ESSources
// Class:      TagCollectionRetriever
//
// Author:      Zhen Xie
//
#include "TagCollectionRetriever.h"
#include "TagDBNames.h"
#include "RelationalAccess/ISchema.h"
#include "RelationalAccess/ITable.h"
#include "RelationalAccess/IQuery.h"
#include "RelationalAccess/ICursor.h"
#include "CoralBase/AttributeList.h"
#include "CoralBase/Attribute.h"
#include "CondCore/DBCommon/interface/CoralTransaction.h"
//#include <iostream>
cond::TagCollectionRetriever::TagCollectionRetriever( cond::CoralTransaction& coraldb ):m_coraldb(&coraldb){
}
cond::TagCollectionRetriever::~TagCollectionRetriever(){}
void 
cond::TagCollectionRetriever::getTagCollection( const std::string& globaltag,
						std::map< std::string, 
						cond::TagMetadata >& result){
  coral::ITable& tagInventorytable=m_coraldb->nominalSchema().tableHandle(cond::TagDBNames::tagInventoryTable());
  coral::IQuery* query=m_coraldb->nominalSchema().newQuery();
  std::pair<std::string,std::string> treenodepair=parseglobaltag(globaltag);
  std::string treename=treenodepair.first;
  std::string nodename=treenodepair.second;
  //std::cout<<"treename "<<treename<<std::endl;
  //std::cout<<"nodename "<<nodename<<std::endl;
  std::string treetablename(cond::TagDBNames::tagTreeTablePrefix());
  if( !treename.empty() ){
    for(unsigned int i=0; i<treename.size(); ++i){
      treename[i]=std::toupper(treename[i]);	
    }
    treetablename+="_";
    treetablename+=treename;
  }
  //std::cout<<"treetablename "<<treetablename<<std::endl;
  query->addToTableList( treetablename, "p1" );
  query->addToTableList( treetablename, "p2" );
  query->addToOutputList( "p1.tagid" );
  query->setRowCacheSize( 100 );
  coral::AttributeList bindData;
  bindData.extend( "nodelabel",typeid(std::string) );
  bindData.extend( "tagid",typeid(unsigned int) );
  bindData["tagid"].data<unsigned int>()=0;
  bindData["nodelabel"].data<std::string>()=nodename;
  query->setCondition( "p1.lft BETWEEN p2.lft AND p2.rgt AND p2.nodelabel = :nodelabel AND p1.tagid <> :tagid", bindData );
  coral::AttributeList qresult;
  qresult.extend("tagid", typeid(unsigned int));
  query->defineOutput(qresult);
  std::vector<unsigned int> leaftagids;
  leaftagids.reserve(100);
  coral::ICursor& cursor = query->execute();
  while( cursor.next() ) {
    const coral::AttributeList& row = cursor.currentRow();
    leaftagids.push_back(row["tagid"].data<unsigned int>());
  }
  cursor.close();
  delete query;
  std::vector<unsigned int>::iterator it;
  std::vector<unsigned int>::iterator itBeg=leaftagids.begin();
  std::vector<unsigned int>::iterator itEnd=leaftagids.end();
  for( it=itBeg; it!=itEnd; ++it ){
    coral::IQuery* leaftagquery=tagInventorytable.newQuery();
    leaftagquery->addToOutputList( "tagname" );
    leaftagquery->addToOutputList( "pfn" );
    leaftagquery->addToOutputList( "recordname" );
    leaftagquery->addToOutputList( "objectname" );
    leaftagquery->addToOutputList( "labelname" );
    leaftagquery->addToOutputList( "timetype" );
    coral::AttributeList myresult;
    myresult.extend("tagname",typeid(std::string));
    myresult.extend("pfn",typeid(std::string));
    myresult.extend("recordname",typeid(std::string));
    myresult.extend("objectname",typeid(std::string));
    myresult.extend("labelname",typeid(std::string));
    myresult.extend("timetype",typeid(std::string));
    leaftagquery->defineOutput( myresult );
    coral::AttributeList bindVariableList;
    bindVariableList.extend("tagid",typeid(unsigned int));
    leaftagquery->setCondition( "tagid = :tagid",bindVariableList );
    leaftagquery->limitReturnedRows(1,0);
    bindVariableList["tagid"].data<unsigned int>()=*it;
    coral::ICursor& cursor2 =leaftagquery->execute();
    if( cursor2.next() ){
      const coral::AttributeList& row = cursor2.currentRow();
      cond::TagMetadata tagmetadata;
      std::string tagname=row["tagname"].data<std::string>();
      tagmetadata.pfn=row["pfn"].data<std::string>();
      tagmetadata.recordname=row["recordname"].data<std::string>();
      tagmetadata.objectname=row["objectname"].data<std::string>();
      tagmetadata.labelname=row["labelname"].data<std::string>();
      tagmetadata.timetype=row["timetype"].data<std::string>();
      result.insert( std::make_pair<std::string, cond::TagMetadata>(tagname,tagmetadata) );
    }
    cursor2.close();
    delete leaftagquery;
  }
}

std::pair<std::string,std::string>
cond::TagCollectionRetriever::parseglobaltag(const std::string& globaltag){
  std::pair<std::string,std::string> result;
  std::size_t pos=globaltag.find("::");
  if(pos==std::string::npos){
    result.first="";
    result.second=globaltag;
  }else{
    result.first=globaltag.substr(0,pos);
    result.second=globaltag.substr(pos+2);
  }
  return result;
}

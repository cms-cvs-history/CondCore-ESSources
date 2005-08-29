#include "CondCore/DBCommon/interface/DBWriter.h"
#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
#include "CondCore/IOVService/interface/IOV.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "SealKernel/Service.h"
#include "POOLCore/POOLContext.h"
#include "SealKernel/Context.h"
#include <string>
#include <map>
#include <iostream>
int main(){
  pool::POOLContext::loadComponent( "SEAL/Services/MessageService" );
  pool::POOLContext::setMessageVerbosityLevel( seal::Msg::Error );
  //cond::DBWriter w(std::string("sqlite_file:test.db")); 
  cond::DBWriter w(std::string("sqlite_file:test.db")); 
  cond::IOV* iov=new cond::IOV;
  w.createContainer("EcalPedestals");
  EcalPedestals* ped1=new EcalPedestals;
  std::cout<<1<<std::endl;
  int channelId;
  EcalPedestals::Item item;
  
  channelId=1878;
  item.m_mean=0.3;
  item.m_variance=0.4;
  ped1->m_pedestals.insert(std::make_pair(channelId,item));
  std::cout<<2<<std::endl;

  channelId=1858;
  item.m_mean=0.5;
  item.m_variance=0.94;
  ped1->m_pedestals.insert(std::make_pair(channelId,item));
  w.startTransaction();
  std::string tok=w.write<EcalPedestals>(ped1);
  std::cout<<"about to commit1"<<std::endl;
  std::cout<<3<<std::endl;
  //assign IOV
  int tillrun=5;
  iov->iov.insert(std::make_pair(tillrun,tok));
  std::string iovToken="akdkgh";
    //w.write<cond::IOV>(iov);
  std::cout<<4<<std::endl;
  /*
  EcalPedestals* ped2=new EcalPedestals; //the user gives up the object ownership upon send it to the writer
  std::cout<<5<<std::endl;
  channelId=1878;
  item.m_mean=0.33;
  item.m_variance=0.44;
  ped2->m_pedestals.insert(std::make_pair(channelId,item));

  channelId=1858;
  item.m_mean=0.56;
  item.m_variance=0.98;
  ped2->m_pedestals.insert(std::make_pair(channelId,item));

  std::string tok2=w.write<EcalPedestals>(ped2);
  //assign IOV
  tillrun=10;
  iov->iov.insert(std::make_pair(tillrun,tok2));
  std::cout<<6<<std::endl;
  std::string iovToken=w.write<cond::IOV>(iov);
  */
  w.commitTransaction();
  std::cout<<"about to commit2"<<std::endl;
  std::cout<<"writercommitted"<<std::endl;
  //register the iovToken to the metadata service
  cond::MetaData metadata_svc("sqlite_file:test.db");
  metadata_svc.addMapping("EcalPedestals_2008_test", iovToken);  
}

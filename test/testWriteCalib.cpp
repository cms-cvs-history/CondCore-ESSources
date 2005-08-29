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
  //std::string contact("oracle://devdb10/cms_xiezhen_dev");
  std::string contact("sqlite_file://ecalcalib.db");
  pool::POOLContext::loadComponent( "SEAL/Services/MessageService" );
  pool::POOLContext::setMessageVerbosityLevel( seal::Msg::Error );
  const std::string userNameEnv = "POOL_AUTH_USER=cms_xiezhen_dev";
  ::putenv( const_cast<char*>( userNameEnv.c_str() ) );
  const std::string passwdEnv = "POOL_AUTH_PASSWORD=xiezhen123";
  ::putenv( const_cast<char*>( passwdEnv.c_str() ) );
  cond::DBWriter w(contact); 
  cond::IOV* iov=new cond::IOV;
  w.createContainer("EcalPedestals");
  EcalPedestals* ped1=new EcalPedestals;
  int channelId;
  EcalPedestals::Item item;
  
  channelId=1878;
  item.m_mean=0.3;
  item.m_variance=0.4;
  ped1->m_pedestals.insert(std::make_pair(channelId,item));
  
  channelId=1858;
  item.m_mean=0.5;
  item.m_variance=0.94;
  ped1->m_pedestals.insert(std::make_pair(channelId,item));
  w.startTransaction();
  std::string tok=w.write<EcalPedestals>(ped1);
  //assign IOV
  int tillrun=5;
  iov->iov.insert(std::make_pair(tillrun,tok));
  
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
  w.createContainer("IOV");
  std::string iovToken=w.write<cond::IOV>(iov);  
  w.commitTransaction();
  std::cout<<"writercommitted"<<std::endl;
  //register the iovToken to the metadata service
  cond::MetaData metadata_svc(contact);
  metadata_svc.addMapping("EcalPedestals_2008_test", iovToken);  
}

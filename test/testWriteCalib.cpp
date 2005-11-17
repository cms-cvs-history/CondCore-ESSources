#include "CondCore/DBCommon/interface/DBWriter.h"
#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
#include "CondCore/IOVService/interface/IOV.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "FWCore/Framework/interface/IOVSyncValue.h"
#include "SealKernel/Service.h"
#include "POOLCore/POOLContext.h"
#include "SealKernel/Context.h"
#include <string>
#include <map>
#include <iostream>
int main(){
  pool::POOLContext::loadComponent( "SEAL/Services/MessageService" );
  pool::POOLContext::setMessageVerbosityLevel( seal::Msg::Error );
  {
  std::string ecalcontact("sqlite_file:ecalcalib.db");
  cond::IOV* ecalpediov=new cond::IOV;
  cond::DBWriter ecalw(ecalcontact);
  ecalw.startTransaction();
  EcalPedestals* ped1=new EcalPedestals;
  int channelId;
  EcalPedestals::Item item;
  
  channelId=1656;
  item.mean_x1 =0.91;
  item.rms_x1  =0.17;
  item.mean_x6 =0.52;
  item.rms_x6  =0.03;
  item.mean_x12=0.16;
  item.rms_x12 =0.05;
  ped1->m_pedestals.insert(std::make_pair(channelId,item));
  
  channelId=1200;
  item.mean_x1=0.5;
  item.rms_x1=0.94;
  item.mean_x6 =0.72;
  item.rms_x6  =0.07;
  item.mean_x12=0.87;
  item.rms_x12 =0.07;
  ped1->m_pedestals.insert(std::make_pair(channelId,item));
  
  std::string ped1tok=ecalw.write<EcalPedestals>(ped1, "EcalPedestals");//pool::Ref takes the ownership of ped1
  std::cout<<"ecalped1 token "<<ped1tok<<std::endl;
  //assign IOV
  unsigned long tillrun=5;
  ecalpediov->iov.insert(std::make_pair(tillrun,ped1tok));
  
  EcalPedestals* ped2=new EcalPedestals; //the user gives up the object ownership upon send it to the writer
  channelId=1656;
  item.mean_x1=0.33;
  item.rms_x1=0.44;
  item.mean_x6 =0.22;
  item.rms_x6  =0.11;
  item.mean_x12=0.39;
  item.rms_x12 =0.12;
  ped2->m_pedestals.insert(std::make_pair(channelId,item));
  
  channelId=1200;
  item.mean_x1=0.56;
  item.rms_x1=0.98;
  item.mean_x6 =0.83;
  item.rms_x6  =0.27;
  item.mean_x12=0.54;
  item.rms_x12 =0.27;
  ped2->m_pedestals.insert(std::make_pair(channelId,item));
  
  std::string pedtok2=ecalw.write<EcalPedestals>(ped2,"EcalPedestals");
  std::cout<<"ecalped2 token "<<pedtok2<<std::endl;
  
  //assign IOV
  tillrun=10;
  ecalpediov->iov.insert(std::make_pair(tillrun,pedtok2));
  std::string ecalpediovToken=ecalw.write<cond::IOV>(ecalpediov,"IOV");  
  std::cout<<"ecalped iov token "<<ecalpediovToken<<std::endl;  
  ecalw.commitTransaction();
  cond::MetaData ecalmetadata_svc(ecalcontact);
  ecalmetadata_svc.addMapping("ecalfall_test", ecalpediovToken);
}
  { 
  std::cout<<"start writing general ped"<<std::endl;
  //write general pedestals
  std::string pedcontact("sqlite_file:calib.db");
  cond::DBWriter pedw(pedcontact);
  pedw.startTransaction();
  cond::IOV* pediov=new cond::IOV;
  Pedestals* myped=new Pedestals;
  Pedestals::Item myitem;
  myitem.m_mean=1.11;
  myitem.m_variance=0.11;
  myped->m_pedestals.push_back(myitem);
  myitem.m_mean=1.12;
  myitem.m_variance=0.21;
  myped->m_pedestals.push_back(myitem);
  std::string mytok=pedw.write<Pedestals>(myped,"Pedestals");
  pediov->iov.insert(std::make_pair(10,mytok));
  std::string pediovToken=pedw.write<cond::IOV>(pediov,"IOV");  
  pedw.commitTransaction();
  std::cout<<"about to write meta data"<<std::endl;
  cond::MetaData metadata_svc(pedcontact);
  metadata_svc.addMapping("fall_test", pediovToken);
  }
}

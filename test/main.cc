// -*- C++ -*-
//
// Package:     E2EContextTest
// Class  :     main
// 
// Implementation:
//     <Notes on implementation>
//
// Author:      Chris Jones
// Created:     Wed May 11 19:38:58 EDT 2005
// $Id: main.cc,v 1.1 2005/07/07 13:24:05 xiezhen Exp $
//

// system include files
#include <iostream>
//#include <sstream>
#include <stdexcept>

#include "FWCore/Framework/interface/EventProcessor.h"
                                                    
int main(int argc, char * argv[] )
{
  std::cout<<1<<std::endl;
  int rc =-1;
  std::cout<<2<<std::endl;
  try {
    std::cout<<3<<std::endl;
    edm::EventProcessor processor(argc,argv);
    std::cout<<4<<std::endl;
    rc=processor.run();
    std::cout<<5<<rc<<std::endl;
    rc=0;
  }catch( const std::exception& iException ) {
    std::cout <<"Caught exception: "<<iException.what() <<std::endl;
    rc =2;
  } catch(...){
    std::cerr << "Failed to create framework object\n"<<std::endl;
  }
  return rc;
}

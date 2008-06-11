#!/usr/bin/env python

"""
An example script that demonstrates usage of pythongrid that uses easysvm.
http://www.easysvm.org

The script computes cross validation error for SVM binary classification with
a linear kernel. The features are two dimensional real vectors (GC content)
"""

#############################################################################################
#                                                                                           #
#    This program is free software; you can redistribute it and/or modify                   #
#    it under the terms of the GNU General Public License as published by                   #
#    the Free Software Foundation; either version 3 of the License, or                      #
#    (at your option) any later version.                                                    #
#                                                                                           #
#    This program is distributed in the hope that it will be useful,                        #
#    but WITHOUT ANY WARRANTY; without even the implied warranty of                         #
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the                           # 
#    GNU General Public License for more details.                                           #
#                                                                                           #
#    You should have received a copy of the GNU General Public License                      # 
#    along with this program; if not, see http://www.gnu.org/licenses                       #
#    or write to the Free Software Foundation, Inc., 51 Franklin Street,                    #
#    Fifth Floor, Boston, MA 02110-1301  USA                                                #
#                                                                                           #
#############################################################################################

import bz2
import time
import sys
from splicesites.utils import create_dataset, create_modsel
from esvm.utils import calcroc, getPartitionedSet, getCurrentSplit
from esvm.experiment import crossvalidation, train_and_test
from esvm.mldata import init_datasetfile
from numpy.linalg import norm
import numpy

from pythongrid import KybJob, processJobs, submitJobs, collectJobs

def demo(gcfilename, plot=False):
    """
    Use pythongrid for cross validation
    """
    # hyperparameters
    num_fold_cv = 5
    C = 1

    # GC features
    fp = init_datasetfile(gcfilename,'vec')
    (gc_examples,gc_labels) = fp.readlines()

    if plot:
        from pylab import scatter,show
        color=['b','r']
        scatter(gc_examples[0,], gc_examples[1,], s=400*(gc_labels+2), \
                c=''.join([ color[(int(i)+1)/2] for i in gc_labels]), alpha=0.1)
        show()

    kernelname = 'linear'
    kparam = {'scale':1.0}

    #(all_outputs, all_split) = crossvalidation(num_fold_cv, kernelname, kparam, C, gc_examples, gc_labels)
    #print 'AROC: %f' % calcroc(all_outputs,gc_labels)

    #####################################################################
    # initialise splits
    #####################################################################
    partitions = getPartitionedSet(len(gc_labels), num_fold_cv)


    demo_forloop(num_fold_cv,partitions,gc_labels,gc_examples,kernelname,kparam,C)
    demo_jobslocal(num_fold_cv,partitions,gc_labels,gc_examples,kernelname,kparam,C)
    demo_jobswait(num_fold_cv,partitions,gc_labels,gc_examples,kernelname,kparam,C)
    demo_session(num_fold_cv,partitions,gc_labels,gc_examples,kernelname,kparam,C)



def demo_forloop(num_fold_cv,partitions,gc_labels,gc_examples,\
                 kernelname,kparam,C):
    """
    normal for loop
    """
    print 'demo for loop'
    svmout = []
    for fold in xrange(num_fold_cv):
        svmout.append(numpy.zeros(len(partitions[fold])))

    for fold in xrange(num_fold_cv):
        XT, LT, XTE, LTE = getCurrentSplit(fold, partitions, gc_labels, gc_examples)
        svmout[fold] = train_and_test(XT, LT, XTE, C, kernelname, kparam)
    report_error(partitions,svmout,gc_labels)



def demo_jobslocal(num_fold_cv,partitions,gc_labels,gc_examples,\
                   kernelname,kparam,C):
    """
    Use pythongrid, but run jobs locally on the same machine.
    This doesn't need DRMAA.
    """
    print 'demo jobs local'
    myjobs = create_jobs(num_fold_cv,partitions,gc_labels,gc_examples,\
                         kernelname,kparam,C)
    processJobs(myjobs, local=True)
    collect_results(myjobs,partitions,gc_labels)


def demo_jobswait(num_fold_cv,partitions,gc_labels,gc_examples,\
                  kernelname,kparam,C):
    """
    Use pythongrid to submit jobs to the cluster,
    and wait for them to complete.
    Needs DRMAA, but code looks like normal for loop.
    """
    print 'demo jobs wait'
    myjobs = create_jobs(num_fold_cv,partitions,gc_labels,gc_examples,\
                         kernelname,kparam,C)
    retjobs = processJobs(myjobs, local=False)
    collect_results(retjobs,partitions,gc_labels)


    
def demo_session(num_fold_cv,partitions,gc_labels,gc_examples,\
                 kernelname,kparam,C):                 
    """
    Use pythongrid to submit jobs to the cluster.
    Submission returns a session id which is used later to
    collect the results.
    Needs DRMAA, and user code has to take care of job completion.
    """
    print 'demo session'
    myjobs = create_jobs(num_fold_cv,partitions,gc_labels,gc_examples,\
                         kernelname,kparam,C)
    (sid,jobids,filenames)=submitJobs(myjobs)
    print 'job submission complete, having a short nap... zzzz'
    del myjobs
    time.sleep(10)
    print 'woken up, collecting jobs'
    myjobs=collectJobs(sid,jobids,filenames)
    collect_results(myjobs,partitions,gc_labels)


def create_jobs(num_fold_cv,partitions,gc_labels,gc_examples,\
                kernelname,kparam,C):
    """create jobs"""
    myjobs = []
    for fold in xrange(num_fold_cv):
        XT, LT, XTE, LTE = getCurrentSplit(fold, partitions, gc_labels, gc_examples)
        myjobs.append(KybJob(train_and_test,(XT, LT, XTE, C, kernelname, kparam),cleanup=False))

    return myjobs


def collect_results(myjobs,partitions,gc_labels):
    """collect results"""
    num_fold_cv = len(myjobs)
    svmout = []
    for fold in xrange(num_fold_cv):
        svmout.append(numpy.zeros(len(partitions[fold])))

    for fold in xrange(num_fold_cv):
        svmout[fold] = myjobs[fold].ret

    report_error(partitions,svmout,gc_labels)



def report_error(partitions,svmout,labels):
    """
    Sort the SVM outputs
    """
    num_fold_cv = len(partitions)
    total_examples = 0
    for fold in xrange(num_fold_cv):
        total_examples += len(partitions[fold])
    all_outputs = [0.0] * total_examples

    for fold in xrange(num_fold_cv):
        for ix in xrange(len(svmout[fold])):
            all_outputs[partitions[fold][ix]] = svmout[fold][ix]

    print 'AROC: %f' % calcroc(all_outputs,labels)
    
if __name__ == '__main__':
    demo('C_elegans_acc_gc.csv')

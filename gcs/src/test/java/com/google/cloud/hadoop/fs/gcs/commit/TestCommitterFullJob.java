/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs.commit;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.fs.gcs.contract.GoogleContract;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.TestJobThroughManifestCommitter;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

/** Test the Manifest committer stages against ABFS. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestCommitterFullJob extends TestJobThroughManifestCommitter {

  private static final AtomicReference<GoogleHadoopFileSystem> fs = new AtomicReference<>();
  private static final GoogleCloudStorageTestHelper.TestBucketHelper TEST_BUCKET_HELPER =
      new GoogleCloudStorageTestHelper.TestBucketHelper(GoogleContract.TEST_BUCKET_NAME_PREFIX);

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new GoogleContract(conf, TEST_BUCKET_HELPER);
  }

  @Override
  protected Configuration createConfiguration() {
    return enableManifestCommitter(super.createConfiguration());
  }

  @AfterClass
  public static void cleanup() throws Exception {
    final GoogleHadoopFileSystem gfs = fs.get();
    if (gfs != null) {
      TEST_BUCKET_HELPER.cleanup(gfs.getGcsFs().getGcs());
    }
  }
}

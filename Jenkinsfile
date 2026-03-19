pipeline {
    agent { label 'transltr-ci-build-node-03-24.04' }
    
    parameters {
        choice(name: 'SOURCE', 
            choices: [
                'all',
                'alliance',
                'bgee',
                'bindingdb',
                'chembl',
                'cohd',
                'ctd',
                'ctkp',
                'dakp',
                'dgidb',
                'diseases',
                'drug_rep_hub',
                'drugcentral',
                'gene2phenotype',
                'geneticskp',
                'go_cam',
                'goa',
                'gtopdb',
                'hpoa',
                'icees',
                'intact',
                'ncbi_gene',
                'panther',
                'pathbank',
                'semmeddb',
                'sider',
                'signor',
                'tmkp',
                'ttd',
                'ubergraph'
            ], 
            description: 'Source to process (all = run all sources)')
        booleanParam(name: 'OVERWRITE', defaultValue: false, description: 'Overwrite existing files')
    }
    
    environment {
        S3_BUCKET_NAME = 'kgx-translator-ingests'
    }
    
    stages {
        stage('Checkout') {
            steps {
                cleanWs()
                checkout scm
            }
        }
        
        stage('Install Dependencies') {
            steps {
                sh 'uv sync'
            }
        }
        
        stage('Run Pipeline and Upload') {
            steps {
                script {
                    def overwriteFlag = params.OVERWRITE ? 'OVERWRITE=true' : ''
                    
                    if (params.SOURCE == 'all') {
                        // Get list of all sources from Makefile
                        def sources = ['alliance', 'bgee', 'bindingdb', 'chembl', 'cohd', 'ctd', 
                                      'ctkp', 'dakp', 'dgidb', 'diseases', 'drug_rep_hub', 
                                      'drugcentral', 'gene2phenotype', 'geneticskp', 'go_cam', 
                                      'goa', 'gtopdb', 'hpoa', 'icees', 'intact', 'ncbi_gene', 
                                      'panther', 'pathbank', 'semmeddb', 'sider', 'signor', 
                                      'tmkp', 'ttd', 'ubergraph']
                        
                        // Run and upload each source individually
                        def results = [:]
                        for (source in sources) {
                            try {
                                // Check if source needs update (unless OVERWRITE is set)
                                def needsUpdate = true
                                if (!params.OVERWRITE) {
                                    def checkResult = sh(
                                        script: "uv run python check_source_needs_update.py ${source}",
                                        returnStatus: true
                                    )
                                    needsUpdate = (checkResult == 0)
                                    
                                    if (!needsUpdate) {
                                        echo "Skipping ${source} - already up to date in S3"
                                        results[source] = 'SKIPPED'
                                        continue
                                    }
                                }
                                
                                echo "Processing ${source}..."
                                sh "make run SOURCES=${source} ${overwriteFlag}"
                                sh "make upload SOURCES=${source}"
                                results[source] = 'SUCCESS'
                            } catch (Exception e) {
                                echo "ERROR: ${source} failed: ${e.message}"
                                results[source] = 'FAILED'
                                // Continue to next source instead of failing the build
                            }
                        }
                        
                        // Report results
                        echo "\n=== Pipeline Results ==="
                        results.each { source, status ->
                            echo "${source}: ${status}"
                        }
                        
                        // Fail build if any source failed
                        if (results.any { it.value == 'FAILED' }) {
                            error("Some sources failed. Check logs above.")
                        }
                    } else {
                        // Run and upload specific source
                        sh "make run SOURCES=${params.SOURCE} ${overwriteFlag}"
                        sh "make upload SOURCES=${params.SOURCE}"
                    }
                }
            }
        }
    }
    
    post {
        cleanup {
            cleanWs()
        }
    }
}
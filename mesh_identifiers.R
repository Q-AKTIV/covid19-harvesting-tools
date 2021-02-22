# Script for mapping of MeSH identifiers from preprints to text MeSH descriptors (medical subject headings)
# Install the required packages: 
install.packages("XML")
install.packages("data.table")
install.packages("tidyr")
install.packages("dplyr")


### Load the complete MeSH Thesaurus (copied from the Medline) into R environment:

library(XML) # load a package to read xml files
# Read the MeSH (2020 edition) thesaurus
mesh2020 <- xmlParse("C:/Users/Tetyana Melnychuk/Documents/Q-Aktiv-Inhalt/2020_COVID-19/Daten/2020_07_02_zbmed_covid/desc2020.xml") # put the path to the MeSH thesaurus file here 
mesh2020_df <- xmlToDataFrame(nodes = getNodeSet(mesh2020,  "//DescriptorRecord")) # converting xml files to data frame object according to nodes "DescriptorRecord" 
#View(mesh2020_df) # optinal view of the created data frame of MeSH thesaurus

# Create a smaller data frame with the columns required DescriptorUI und TreeNumberList from the MesH-Thesaurus:
mesh_desc_ui <- subset(mesh2020_df, select = c(DescriptorUI,TreeNumberList,DescriptorName))
# View(mesh_desc_ui) optinal view of the created data frame

# Convert the data frame into a data table object for adding the keys with the help of which the MeSH thesaurus
# is going to be mapped with the preprints' identifiers:
library(data.table)
names(mesh_desc_ui)[names(mesh_desc_ui) == 'DescriptorUI'] <- 'descriptor' # renaming the column DescriptorUI to descriptor
mesh_desc_ui <- as.data.table(mesh_desc_ui) # convert a data frame to data table object
setkey(mesh_desc_ui, descriptor) # setting keys: a column descriptor is a key 

# Read preprints data containing MeSH identifiers:
daten_preprints <- read.csv('C:/Users/Tetyana Melnychuk/Documents/Q-Aktiv-Inhalt/2020_COVID-19/Daten/2021_01_neuer_ds/annotation_unmapped.csv',  
                            header=T, dec=".",sep=",", quote = '"') 
#head(daten_preprints) # optinal view of the read data

# Cleaning the string "http://id.nlm.nih.gov/mesh/" in the identifiers' column.
# Unter Umstaenden muessen die Konzept noch bereinigt werden.
daten_preprints$subject <- gsub("http://id.nlm.nih.gov/mesh/", "", daten_preprints$concept) # removing http://id.nlm.nih.gov/mesh/ and creating a new column subject with MeSH cleaned identifiers only

daten_preprints <- as.data.table(daten_preprints) # convert a data frame to data table object
setkey(daten_preprints,subject) # setting a key subject (which is a column with the MeSH identifiers (descriptors), e.g. D008031)

# Mapping of MeSH descriptors:
daten_preprints_m <- merge(daten_preprints, mesh_desc_ui, by.x = c("subject"), by.y = c("descriptor"), all.x = TRUE)
#View(daten_preprints_m) # optinal view of the mapped data

# Removing observations containing NAs (mostly, these NAs are MeSH Qualifiers or Supplementary Concept Record Terms)
library(tidyr)
daten_preprints_m <- daten_preprints_m %>% drop_na(DescriptorName)

# Removing duplicates:
library(dplyr)
daten_preprints_m_d <- daten_preprints_m %>% 
  select(subject,paper_id,DescriptorName,TreeNumberList) %>% # Here, 4 columns are selected for processing 
  distinct %>% # here the duplicates are removed (only distinct observations are selected)
  select(paper_id,DescriptorName,TreeNumberList) # here, only the relevant columns are selected as output: paper_id, DescriptorName (MeSH-Descriptor) and TreeNumberList (hierarchy levels relationship) 
View(daten_preprints_m_d) # optional view of the created data set

# Export the ready data set as a csv file:
write.table(daten_preprints_m_d, file = 'C:/Users/Tetyana Melnychuk/Documents/Q-Aktiv-Inhalt/2020_COVID-19/Daten/2021_01_neuer_ds/annotation_mapped.csv',
            append = FALSE, sep = ",", dec = ".", quote = TRUE,
            row.names = FALSE, col.names = TRUE)


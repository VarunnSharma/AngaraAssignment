const bodyparser = require("body-parser");
const fs = require("fs");
const path = require("path");
const multer = require("multer");
const csv = require("csv-parser");
const express = require("express");
const zlib = require("zlib");

const app = express();
const gzip = zlib.createGzip();
const PORT = 5000;

const uploads = multer({
  storage: storage,
});
let structure = { folder: "", files: [] };

fs.mkdir("./uploadedFiles", function (err) {
  if (err) {
    console.log("uploadedFiles already present");
  } else {
    console.log("uploadedFiles directory successfully created.");
  }
});

fs.mkdir("./compressedFiles", function (err) {
  if (err) {
    console.log("compressedFiles already present");
  } else {
    console.log("compressedFiles directory successfully created.");
  }
});

app.use(bodyparser.json());
app.use(
  bodyparser.urlencoded({
    extended: true,
  })
);

app.listen(PORT, () => console.log(`Node app serving on port: ${PORT}`));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "/UploadFile.html"));
});

app.get("/import-csv", (req, res) => {
  res.send(JSON.stringify(csvDataColl));
});

app.post("/import-csv", uploads.array("files"), (req, res) => {
  req.files.forEach((file) => {
    uploadCsv(path.join(__dirname, "/uploadedFiles/", file.originalname));
  });
  res.status({
    status: `data for ${req.files.length} files added successfully`,
  });
});

const storage = multer.diskStorage({
  destination: (req, file, callBack) => {
    callBack(null, path.join(__dirname, "/uploadedFiles/"));
  },
  filename: (req, file, callBack) => {
    callBack(null, file.originalname);
  },
});

async function compressNewFiles(workingDir) {
  const items = await fs.readdirSync(workingDir, { withFileTypes: true });
  items.forEach((item) => {
    if (!structure.files.includes(item)) {
      console.log(item.name, " is new item");
      const inp = fs.createReadStream(workingDir + "/" + item.name);
      const out = fs.createWriteStream("compressedFiles/" + item.name + ".gz");
      inp.pipe(gzip).pipe(out);
      var dateTime = new Date();
      console.log(
        item.name,
        " is zipped at ",
        dateTime,
        " using deflate method compression algorithm"
      );
    }
  });
}

async function getDirDetails(workingDir) {
  const items = await fs.readdirSync(workingDir, { withFileTypes: true });
  let fileName = workingDir;
  items.forEach((item) => {
    if (item.isDirectory()) {
      fileName.concat(getDirDetails(`${workingDir}/${item.name}`));
    } else {
      structure.folder = fileName;
      structure.files.push(item.name);
    }
  });
}

let csvDataColl = [];
function uploadCsv(uriFile) {
  fs.createReadStream(uriFile)
    .pipe(csv({}))
    .on("data", (data) => {
      csvDataColl.push(data);
    })
    .on("end", () => {
      getData(csvDataColl);
    });
}

function getData(data) {
  let rowSize;
  let columnSize;
  let firstRow = data[0];
  columnSize = firstRow != undefined ? Object.keys(data[0]).length : 0;
  rowSize = data.length;
  for (let a in firstRow)
    console.log(`Data type of ${a} is ${typeof firstRow[a]}`);

  console.log("Number of rows--------->", rowSize);
  console.log("Number of columns--------->", columnSize);
  getDirDetails("UploadedFiles");
  compressNewFiles("UploadedFiles");
}

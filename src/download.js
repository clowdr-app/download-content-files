const { program } = require('commander');
const http = require('http'); // or 'https' for https:// URLs
const fs = require('fs');
const papa = require("papaparse");
const path = require("path");
const process = require("process");
const { transcode } = require('buffer');
const AmazonS3URI = require('amazon-s3-uri')

program
    .name("Clowdr (Midspace) content files downloader")
    .description("A tool to download each file from your Clowdr (Midspace) content export.")
    .version("1.0.0");

program
    .requiredOption('-i, --input <file>', "Content CSV file as exported from Clowdr (Midspace)")
    .requiredOption('-o, --output <dir>', "Output directory for the downloaded files")
    .option("-s, --skip-videos", "Skip video files");

program.parse();

function convertToValidFilename(string) {
    return string.replace(/[\\/:"*?<>|\.]+/g, "-").replace(/  /g, " ").trim();
}

const options = program.opts();

const csvFilePath = options.input;
const outputDirPath = options.output;
const skipVideos = Boolean(options.skipVideos);

if (!fs.existsSync(outputDirPath)) {
    fs.mkdirSync(outputDirPath, { recursive: true });
}

const csvText = fs.readFileSync(csvFilePath).toString();
const csvData = papa.parse(csvText);

const columnHeadings = csvData.data[0];
const idColumnIdx = columnHeadings.findIndex(x => x === "Content Id");
const titleColumnIdx = columnHeadings.findIndex(x => x === "Title");
const elementIdIdxs = columnHeadings.filter(x => x.startsWith("Element") && x.endsWith("Id")).map(x => columnHeadings.indexOf(x));
const elementNameIdxs = columnHeadings.filter(x => x.startsWith("Element") && x.endsWith("Name")).map(x => columnHeadings.indexOf(x));
const elementTypeIdxs = columnHeadings.filter(x => x.startsWith("Element") && x.endsWith("Type")).map(x => columnHeadings.indexOf(x));
const elementDataIdxs = columnHeadings.filter(x => x.startsWith("Element") && x.endsWith("Data")).map(x => columnHeadings.indexOf(x));

async function downloadFile(s3Url, path, name) {
    if (fs.existsSync(path)) {
        process.stdout.write(`    Skipping existing ${name}\n`);
        return;
    }

    process.stdout.write(`    Downloading ${name}\n`);

    await new Promise((resolve, reject) => {
        try {
            const region = "eu-west-1";
            const { bucket, key } = new AmazonS3URI(s3Url);
            //             process.stdout.write(`Element file info:
            //     Path: ${path}
            //     S3 Url: ${s3Url}

            //     Region: ${region}
            //     Bucket: ${bucket}
            //     Key: ${key}
            // `);
            const file = fs.createWriteStream(path);
            const httpUrl = `http://${bucket}.s3.${region}.amazonaws.com/${key}`;
            http.get(httpUrl, (res) => {
                const { statusCode } = res;
                const contentType = res.headers['content-type'];

                // Any 2xx status code signals a successful response but
                // here we're only checking for 200.
                if (statusCode < 200 || statusCode >= 300) {
                    // Consume response data to free up memory
                    res.resume();
                    reject(new Error('Request Failed.\n' +
                        `Status Code: ${statusCode}`));
                    return;
                }

                process.stdout.write("    [.");
                const tId = setInterval(() => {
                    process.stdout.write(".");
                }, 5000);
                res.pipe(file)
                    .on("finish", () => {
                        clearInterval(tId);
                        process.stdout.write("] 100%\n");
                        resolve();
                    });
            }).on('error', (e) => {
                reject(`Got error: ${e.message}`);
            });
        }
        catch (e) {
            reject(e);
        }
    });
}

async function parseElement(elementIdx, elementCSVData, basePath) {
    const elementId = elementCSVData[elementIdIdxs[elementIdx]];
    const elementName = elementCSVData[elementNameIdxs[elementIdx]];
    const elementType = elementCSVData[elementTypeIdxs[elementIdx]];
    const elementDataStr = elementCSVData[elementDataIdxs[elementIdx]];
    if (!elementDataStr.length) {
        return;
    }
    const elementData = JSON.parse(elementDataStr).data;
    let elementPath = path.join(basePath, `./${elementId} - ${convertToValidFilename(elementName)}`);
    let elementPath2 = elementPath;

    let s3Url;
    let secondS3Url;
    switch (elementType) {
        case 'ABSTRACT':
        case 'TEXT':
            elementPath = elementPath + ".md";
            process.stdout.write(`    Writing ${elementName}\n`);
            if (fs.existsSync(elementPath)) {
                fs.rmSync(elementPath);
            }
            fs.writeFileSync(elementPath, elementData.text);
            return;
        case 'VIDEO_URL':
        case 'VIDEO_LINK':
            return;
        case 'POSTER_FILE':
            {
                s3Url = elementData.s3Url;
                const parts = elementData.s3Url.split(".");
                elementPath += "." + parts[parts.length - 1];
            }
            break;
        case 'POSTER_URL':
            return;
        case 'IMAGE_FILE':
            {
                s3Url = elementData.s3Url;
                const parts = elementData.s3Url.split(".");
                elementPath += "." + parts[parts.length - 1];
            }
            break;
        case 'IMAGE_URL':
            return;
        case 'PAPER_FILE':
            {
                s3Url = elementData.s3Url;
                const parts = elementData.s3Url.split(".");
                elementPath += "." + parts[parts.length - 1];
            }
            break;
        case 'PAPER_URL':
        case 'PAPER_LINK':
            return;
        case 'LINK':
        case 'LINK_BUTTON':
            return;
        case 'VIDEO_FILE':
        case 'AUDIO_FILE':
        case 'VIDEO_BROADCAST':
        case 'VIDEO_PREPUBLISH':
        case 'VIDEO_TITLES':
        case 'VIDEO_SPONSORS_FILLER':
        case 'VIDEO_FILLER':
        case 'VIDEO_COUNTDOWN':
            {
                if (skipVideos && elementType !== "AUDIO_FILE") {
                    return;
                }

                s3Url = elementData.s3Url;
                secondS3Url = elementData.subtitles?.en_US?.s3Url;

                const parts = s3Url.split(".");
                elementPath += "." + parts[parts.length - 1];

                if (secondS3Url) {
                    const parts2 = secondS3Url.split(".");
                    elementPath2 += "." + parts2[parts2.length - 1];
                }
            }
            break;
        case 'ZOOM':
        case 'CONTENT_GROUP_LIST':
        case 'WHOLE_SCHEDULE':
        case 'LIVE_PROGRAM_ROOMS':
        case 'ACTIVE_SOCIAL_ROOMS':
        case 'DIVIDER':
        case 'SPONSOR_BOOTHS':
        case 'EXPLORE_PROGRAM_BUTTON':
        case 'EXPLORE_SCHEDULE_BUTTON':
            return;
        case 'AUDIO_URL':
        case 'AUDIO_LINK':
            return;
    }

    await downloadFile(s3Url, elementPath, elementName + " - " + elementPath.substring(elementPath.lastIndexOf(".") + 1).toUpperCase());
    if (secondS3Url) {
        await downloadFile(secondS3Url, elementPath2, elementName + " - " + elementPath2.substring(elementPath2.lastIndexOf(".") + 1).toUpperCase());
    }
}

async function parseContentItem(csvRow) {
    const itemId = csvRow[idColumnIdx];
    const itemTitle = convertToValidFilename(csvRow[titleColumnIdx]);
    const itemPath = path.join(outputDirPath, `./${itemId} - ${itemTitle}`);

    process.stdout.write(`\nProcessing ${itemId} - ${itemTitle}` + "\n");
    if (!fs.existsSync(itemPath)) {
        fs.mkdirSync(itemPath);
    }

    for (let idx = 0; idx < elementIdIdxs.length; idx++) {
        await parseElement(idx, csvRow, itemPath);
    }
}

async function main() {
    let isFirst = true;
    for (const csvRow of csvData.data) {
        // Skip column headers
        if (isFirst) {
            isFirst = false;
            continue;
        }

        await parseContentItem(csvRow);
    }
}

main();
